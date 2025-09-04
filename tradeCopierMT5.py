
"""
SiRiX -> MT5 Trade Copier (Polling Version, Hardened)
===================================================

This script polls a *master* trader on a SiRiX trading server and mirrors (copies)
that trader's **open / close / size-change trade actions** into a *follower* MetaTrader 5 (MT5) account.

Copies live trading activity from a SiRiX "master" user into an MT5 "follower"
demo account:

- Open: Follower trade created when master opens.
- Close: Follower closed when master closes.
- Size change: Follower scaled.
- SL/TP sync: Copied on open; later changes detected & updated.
- Fixed follower lot size (0.10) regardless of master size (configurable).
- Optional symbol translation (e.g., NQ100 -> NAS100.i).

SAFE FIRST: run on demo, confirm mapping, then go live.

----------------------------------
IMPORTANT BEFORE LIVE / DEMO USE
----------------------------------
1. Launch MT5 and log in to the follower account *before* running this script.
2. Configure `sirix_token`, `master_user_id`, `symbol_map`, and sizing mode below.
3. Start in `dry_run = True` (used as logging only) until confident flipping real orders.
4. Use a *demo* follower account first.

----------------------------------
LIMITATIONS
----------------------------------
~ No persistence across restarts (link_map lost). Use DB/JSON if needed.
~ Pending order mirroring disabled by default.
~ No retry/backoff on network errors (add for production).
~ Symbol/contract conversion must be customised.

---------------------------------------------------
REQUIRED SETUP
---------------------------------------------------
1. MT5 installed & running on this machine.
2. Demo account credentials (below) are correct OR MT5 terminal is already
   logged into the follower account (if you set auto_login=False).
3. Valid SiRiX bearer token + master_user_id.
4. Symbols visible in MT5 "Market Watch" (script will try to select).
"""


import os
import time
import math
import signal
import logging
from dataclasses import dataclass
from typing import Any, Dict, Optional, List, Tuple
from datetime import datetime
import requests
import MetaTrader5 as mt5


# ======================= CONFIG ===============================

# --- Logging ---
# Basic console logging via log() below; optionally configure Python logging.
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

# --- SiRiX API ---
sirix_api_url = "https://restapi-real3.sirixtrader.com/api/UserStatus/GetUserTransactions"
sirix_token = os.getenv("SIRIX_TOKEN", "t1_a7xeQOJPnfBzuCncH60yjLFu")  # <-- demo fallback; override via env
master_user_id = os.getenv("SIRIX_MASTER_USER_ID", "214422")  # string per API model

# Which sections to request from SiRiX (True = ask server to include)
req_open_positions = True
req_pending_positions = False       # enable to mirror pendings
req_closed_positions = True         # needed to detect closes reliably during reconnect
req_monetary_transactions = False   # off for speed (not needed for copying)

# --- Polling ---
poll_interval_sec = float(os.getenv("SIRIX_POLL_INTERVAL_SEC", "1.0"))  # tune 0.5..2.0

# --- MT5 follower account credentials ---
# If MT5 terminal is already logged in, and want to attach to it,
# set auto_login = False. Otherwise, fill these in and leave True.
auto_login = True
mt5_login = int(os.getenv("MT5_LOGIN", "10803859")) or 0        # login account number
mt5_password = os.getenv("MT5_PASSWORD", "3^iz1GVX")             # login password
mt5_server = os.getenv("MT5_SERVER", "VantageGlobalPrimeLLP-Demo")                 # broker server

# --- MT5 Trade settings ---
magic_number = 86543210          # identify copier trades
max_deviation_points = 20        # price slippage tolerance when sending orders
copy_stops = True                # copy SL/TP if provided & valid
validate_stops = True            # check against broker min stop distance
close_on_master_close = True     # auto-close follower when master closes
copy_pending_orders = False      # mirror new pending orders (advanced)
dry_run = False                  # True = log only, set True to test without sending trades

# One-time summary printed after first poll
show_preview_table = True

# --- Volume Sizing Rules ---
# How to convert master position size -> follower MT5 lots.
# Options: "1to1", "fixed", "multiplier", "equity_ratio"
volume_mode = "fixed"

# When volume_mode == "fixed", always trade this many lots
lot_rule_fixed_lots = 0.10

# When volume_mode == "multiplier", follower_lots = master_size_converted * lot_rule_multiplier
lot_rule_multiplier = 1.0

# When volume_mode == "equity_ratio", follower_lots = master_size_converted * (follower_equity / master_equity)
# (master_equity read from SiRiX user data; follower_equity from MT5)

# Master amount interpretation: how to read SiRiX `Amount`
# "units" = raw units, "lots" = lots already, "contracts" = treat as 1:1 lots
master_amount_mode = "units"

# Symbol-specific contract sizing (how many *units* per 1.0 MT5 lot)
# Used when master_amount_mode == "units". Defaults to 100k for FX if not found.
symbol_units_per_lot: Dict[str, float] = {
    "EURUSD": 100_000,
    "GBPUSD": 100_000,
    "XAUUSD": 100,      # 100 oz per lot is common; adjust if your Sirix contract differs
    "NQ100.": 10,        # placeholder; since fixed lots, not critical now
}

# If follower broker contract differs from SiRiX, override here (MT5 symbol -> units/lot)
follower_units_per_lot: Dict[str, float] = {
    # Only needed when contract NOT 1:1; skip for now since fixed lots.
    # "XAUUSDm": 100,  # example
}

# Simple SiRiX -> MT5 symbol mapping. Update to match follower broker symbols.
symbol_map: Dict[str, str] = {
    "NQ100.": "NAS100.i",
    "GER40.": "GER40.i",

}

# If True, warn when a SiRiX symbol not mapped; if False, attempt passthrough
warn_on_unmapped_symbol = True

# --- Sirix side encoding (IMPORTANT) ---
# From live test: follower trades flipped vs master => SiRiX appears 0=BUY, 1=SELL.
sirix_buy_value  = 0
sirix_sell_value = 1

# How sensitive should we be to SL/TP changes? Absolute price delta threshold.
# If change < this, ignore (avoids noise from rounding).
stop_change_abs_tolerance = 1e-6  # adjust if needed (e.g., 0.01 for gold)

# ANSI italic hint string for console readability
side_hint_italic = f"\x1b[3mSide {sirix_buy_value} = BUY | Side {sirix_sell_value} = SELL\x1b[0m"


# =============== GLOBAL RUNTIME STATE ========================

running = True                              # flipped False on SIGINT/SIGTERM
last_sirix_equity: Optional[float] = None   # updated each poll from SiRiX
preview_printed = False                     # track preview output

@dataclass
class MasterPosition:
    """ Representation of a master (SiRiX) open position snapshot. """
    order_number: Any
    symbol: str
    side: int                               # 0=BUY, 1=SELL
    amount: float
    open_time: Optional[str] = None
    open_rate: Optional[float] = None
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None


@dataclass
class FollowerLink:
    """
    Link between a master order and follower MT5 position.
    side holds the *master* direction: 0=BUY,1=SELL.
    follower_volume tracks current MT5 volume (lots).
    """
    master_order_number: Any
    master_symbol: str
    follower_symbol: str
    follower_ticket: int
    follower_volume: float          # lots
    side: int                       # master side: 0=BUY,1=SELL
    sl: Optional[float] = None      # last applied follower SL
    tp: Optional[float] = None      # last applied follower TP

# In-memory mapping: master_order_number -> FollowerLink
link_map: Dict[Any, FollowerLink] = {}

# Latest known master open positions snapshot (order_number -> MasterPosition)
master_open_snapshot: Dict[Any, MasterPosition] = {}


# ==================== UTILITY HELPERS ========================

def log(msg: str, level: int = logging.INFO) -> None:
    """ Basic timestamped console logger using logging module. """
    logging.log(level, msg)

def sirix_is_buy(side: int) -> bool:
    """Return True if raw SiRiX side code means BUY (uses sirix_buy_value)."""
    return side == sirix_buy_value

def sirix_is_sell(side: int) -> bool:
    """Return True if raw SiRiX side code means SELL."""
    return side == sirix_sell_value

def sirix_dir_01(side: int) -> int:
    """Normalize raw SiRiX side code -> canonical 1=buy, 0=sell."""
    return 1 if sirix_is_buy(side) else 0

def sirix_dir_char(side: int) -> str:
    """Return 'B' or 'S' for display."""
    return "B" if sirix_is_buy(side) else "S"


def get_units_per_lot(symbol: str) -> float:
    """ Return approximate units per 1.0 lot for size conversion. """
    return symbol_units_per_lot.get(symbol, 100_000.0)  # default FX size


def get_follower_units_per_lot(symbol: str) -> float:
    """ Return follower broker units/lot override if available; else fall back to master. """
    return follower_units_per_lot.get(symbol, get_units_per_lot(symbol))


def calc_equity_ratio() -> float:
    """ Follower_equity / master_equity. Falls back to 1.0 if unknown. """
    global last_sirix_equity
    try:
        follower_equity = mt5.account_info().equity if not dry_run else None
    except Exception:
        follower_equity = None
    master_eq = last_sirix_equity
    if not follower_equity or not master_eq or master_eq <= 0:
        return 1.0
    return float(follower_equity) / float(master_eq)


def convert_master_amount_to_lots(master_symbol: str,
                                  master_amount: float,
                                  follower_symbol: Optional[str] = None) -> float:
    """
    Convert SiRiX position size to MT5 lots.
    If follower_symbol supplied, prefer follower contract sizing.
    """
    if master_amount is None:
        return 0.0

    # Contract basis
    if master_amount_mode == "lots":
        base_lots = float(master_amount)
    elif master_amount_mode == "units":
        if follower_symbol and follower_symbol in follower_units_per_lot:
            units_per_lot = follower_units_per_lot[follower_symbol]
        else:
            units_per_lot = get_units_per_lot(master_symbol)
        base_lots = float(master_amount) / float(units_per_lot)
    else:  # contracts
        base_lots = float(master_amount)

    # apply volume mode
    if volume_mode == "1to1":
        lots = base_lots
    elif volume_mode == "fixed":
        lots = lot_rule_fixed_lots
    elif volume_mode == "multiplier":
        lots = base_lots * lot_rule_multiplier
    elif volume_mode == "equity_ratio":
        lots = base_lots * calc_equity_ratio()
    else:
        lots = base_lots

    return max(lots, 0.0)


def normalize_lot(symbol: str, lots: float) -> float:
    """ Round lots down to broker's permitted volume step. """
    if dry_run:
        return round(lots, 2)
    info = mt5.symbol_info(symbol)
    if info is None:
        return round(lots, 2)
    step = getattr(info, "volume_step", 0.01) or 0.01
    min_vol = getattr(info, "volume_min", 0.01) or 0.01
    max_vol = getattr(info, "volume_max", 100.0) or 100.0
    lots = math.floor(lots / step) * step  # round down to step
    lots = max(min_vol, min(lots, max_vol))
    return lots


def extract_stops(pos: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
    """ Extract SL/TP from SiRiX open/closed/pending item. """
    sl = pos.get("StopLoss")
    tp = pos.get("TakeProfit")
    try:
        sl = float(sl) if sl not in (None, "", 0) else None
    except Exception:
        sl = None
    try:
        tp = float(tp) if tp not in (None, "", 0) else None
    except Exception:
        tp = None
    return sl, tp


def validate_stops_for_mt5(symbol: str,
                           side: int,
                           dir01: int,           # 1=buy, 0=sell (canonical direction)
                           sl: Optional[float],
                           tp: Optional[float],
                           entry_price: float) -> Tuple[Optional[float], Optional[float]]:
    """
    Drop stops that violate broker min stop distance or direction sanity.
    Validate/clean SL & TP for MT5 broker:
      ~ Direction sanity (SL below/above depending on dir01).
      ~ Minimum stop distance (stop_level -> points -> price).
      ~ Round to symbol precision (digits).
    Return (sl, tp) possibly set to None if invalid.
    side: 1=buy,0=sell
    """
    if not validate_stops:
        return sl, tp
    if dry_run:
        return sl, tp

    info = mt5.symbol_info(symbol)
    if info is None:
        return sl, tp

    point = getattr(info, 'point', 0.0) or 0.0
    stop_level_points = getattr(info, 'stop_level', 0) or 0
    min_dist = stop_level_points * point if point else 0.0
    digits = getattr(info, 'digits', None)

    # SL checks
    if sl is not None:
        if dir01 == 1 and sl >= entry_price:  # BUY -> SL must be below
            sl = None
        elif dir01 == 0 and sl <= entry_price:  # SELL -> SL must be above
            sl = None
        elif min_dist and abs(entry_price - sl) < min_dist:
            sl = None

    # TP checks
    if tp is not None:
        if dir01 == 1 and tp <= entry_price:  # BUY -> TP must be above
            tp = None
        elif dir01 == 0 and tp >= entry_price:  # SELL -> TP must be below
            tp = None
        elif min_dist and abs(entry_price - tp) < min_dist:
            tp = None

    # Broker precision
    if digits is not None and digits >= 0:
        if sl is not None:
            sl = round(sl, digits)
        if tp is not None:
            tp = round(tp, digits)

    return sl, tp


# ===================== MT5 HELPERS ===========================

def side_to_mt5_order_type(side: int) -> int:
    """Map SiRiX side to MT5 order type."""
    return mt5.ORDER_TYPE_BUY if sirix_is_buy(side) else mt5.ORDER_TYPE_SELL


def side_to_reverse_order_type(side: int) -> int:
    """Opposite of side_to_mt5_order_type()."""
    return mt5.ORDER_TYPE_SELL if sirix_is_buy(side) else mt5.ORDER_TYPE_BUY


def mt5_symbol_prepare(symbol: str) -> bool:
    if dry_run:
        return True
    info = mt5.symbol_info(symbol)
    if info and info.visible:
        return True
    if mt5.symbol_select(symbol, True):
        return True
    log(f"[ERR] Unable to select MT5 symbol {symbol}.", logging.ERROR)
    return False


def mt5_current_prices(symbol: str) -> Tuple[Optional[float], Optional[float]]:
    if dry_run:
        return 1.0, 1.0     # dummy
    tick = mt5.symbol_info_tick(symbol)
    if not tick:
        return None, None
    return tick.bid, tick.ask


# ==================== FILLING MODE HELPERS ========================

# Cache of symbol -> bool warned to avoid repeated log spam
_warned_unmapped: Dict[str, bool] = {}
_symbol_filling_cache: Dict[str, int] = {}

def map_symbol(sirix_symbol: str) -> str:
    """Return mapped MT5 symbol; warn once if passthrough."""
    if sirix_symbol in symbol_map:
        return symbol_map[sirix_symbol]
    if warn_on_unmapped_symbol and not _warned_unmapped.get(sirix_symbol):
        log(f"[WARN] No mapping for symbol {sirix_symbol}; using same name.")
        _warned_unmapped[sirix_symbol] = True
    return sirix_symbol

def pick_filling_mode(symbol: str) -> int:
    """
    Pick an MT5 filling mode that the broker supports for this symbol.

    Strategy:
      1. Use cached decision if available.
      2. Inspect symbol_info(...).filling_mode (single allowed mode when returned).
      3. Fallback trial sequence: FOK -> IOC -> RETURN; remember the first that works.

    Returns an mt5.ORDER_FILLING_* constant.
    """
    # cached?
    if symbol in _symbol_filling_cache:
        return _symbol_filling_cache[symbol]

    default_seq = [mt5.ORDER_FILLING_FOK, mt5.ORDER_FILLING_IOC, mt5.ORDER_FILLING_RETURN]

    info = mt5.symbol_info(symbol) if not dry_run else None
    if info is not None:
        # Some brokers report exactly one supported mode here.
        reported = getattr(info, "filling_mode", None)
        if reported in default_seq:
            _symbol_filling_cache[symbol] = reported
            log(f"[filling] Using broker‑reported filling {reported} for {symbol}.")
            return reported

    # If broker didn’t tell us, we’ll try each on first live order send.
    # Temporarily store FOK; actual working mode will be confirmed in _order_send_with_fallback.
    _symbol_filling_cache[symbol] = mt5.ORDER_FILLING_FOK
    return mt5.ORDER_FILLING_FOK

def _order_send_with_fallback(request: dict, symbol: str) -> "mt5.OrderSendResult":
    """
    Send order; if we get 'unsupported filling' (retcode=10030) try next mode.
    Update cache on success.
    """
    if dry_run:
        # fabricate a result‑like object
        class _Dummy:
            retcode = mt5.TRADE_RETCODE_DONE
            order = -1
            deal = -1
        return _Dummy()

    # candidate sequence
    seq = [pick_filling_mode(symbol)]
    # ensure all modes present once
    for m in (mt5.ORDER_FILLING_FOK, mt5.ORDER_FILLING_IOC, mt5.ORDER_FILLING_RETURN):
        if m not in seq:
            seq.append(m)

    last_result = None
    for mode in seq:
        request["type_filling"] = mode
        result = mt5.order_send(request)
        last_result = result
        if result.retcode == 10030:  # unsupported filling mode
            log(f"[filling] {symbol} rejected mode={mode}; trying next...", logging.WARNING)
            continue
        # success or other error -> stop
        if result.retcode in (
            mt5.TRADE_RETCODE_DONE,
            mt5.TRADE_RETCODE_PLACED,
            mt5.TRADE_RETCODE_DONE_PARTIAL,
        ):
            _symbol_filling_cache[symbol] = mode  # remember working mode
        return result

    return last_result


# ==================== MT5 INTERACTION ========================

def init_mt5() -> bool:
    """ Initialize and (optionally) log in. """
    if dry_run:
        log("[init] DRY RUN: skipping MT5 initialize")
        return True
    if not mt5.initialize():
        log(f"[ERR] MT5 initialize failed: {mt5.last_error()}", logging.ERROR)
        return False

    if auto_login:
        if not (mt5_login and mt5_password and mt5_server):
            log("[ERR] auto_login True but credentials missing.", logging.ERROR)
            return False
        if not mt5.login(login=mt5_login, password=mt5_password, server=mt5_server):
            log(f"[ERR] MT5 login failed: {mt5.last_error()}", logging.ERROR)
            return False
        log(f"[init] Logged into MT5 account {mt5_login} ({mt5_server}).")

    else:
        log("[init] Attached to currently logged-in MT5 terminal.")

    return True


def shutdown_mt5() -> None:
    """ Shutdown MT5 connection (if not dry run). """
    if not dry_run:
        mt5.shutdown()


# ================== MT5 TRADE OPERATIONS =====================

def mt5_open_trade(master_pos: MasterPosition,
                   lots: float,
                   sl: Optional[float],
                   tp: Optional[float]) -> Optional[int]:
    """ Send a market trade to MT5 to mirror a master open position. """
    symbol = map_symbol(master_pos.symbol)
    if not mt5_symbol_prepare(symbol):
        return None

    lots = normalize_lot(symbol, lots)
    if lots <= 0:
        log(f"[ERR] Computed non-positive lot size for {symbol}; skipping open.", logging.ERROR)
        return None

    side = side_to_mt5_order_type(master_pos.side)
    bid, ask = mt5_current_prices(symbol)
    if bid is None or ask is None or bid <= 0 or ask <= 0:
        log(f"[ERR] No valid price for {symbol}; cannot trade.", logging.ERROR)
        return None
    price = ask if side == mt5.ORDER_TYPE_BUY else bid

    # Copy stops if config allows; otherwise None
    if not copy_stops:
        sl = None
        tp = None
    else:
        sl, tp = validate_stops_for_mt5(symbol, master_pos.side, sirix_dir_01(master_pos.side), sl, tp, price)

    open_request = {
        "action": mt5.TRADE_ACTION_DEAL,
        "symbol": symbol,
        "volume": lots,
        "type": side,
        "price": price,
        "deviation": max_deviation_points,
        "magic": magic_number,
        "comment": f"Copy SiRiX {master_pos.order_number}",
        "type_time": mt5.ORDER_TIME_GTC,
        "type_filling": mt5.ORDER_FILLING_FOK,
    }
    if sl is not None:
        open_request["sl"] = sl
    if tp is not None:
        open_request["tp"] = tp

    if dry_run:
        log(f"[DRY] Would open {symbol} {lots} lots side={master_pos.side} @ {price} (SL={sl} TP={tp}).")
        return -1  # fake ticket

    result = _order_send_with_fallback(open_request, symbol)
    if result.retcode not in (mt5.TRADE_RETCODE_DONE,
                              mt5.TRADE_RETCODE_PLACED,
                              mt5.TRADE_RETCODE_DONE_PARTIAL):
        log(f"[ERR] MT5 open failed ({symbol}): retcode={result.retcode} details={result}", logging.ERROR)
        return None

    # Use deal if order missing
    ticket = result.order if result.order else result.deal
    log(f"[MT5] Opened {symbol} {lots} lots (ticket {ticket}) for master {master_pos.order_number}.")
    return ticket


def mt5_close_trade(link: FollowerLink) -> bool:
    """ Close follower trade by sending opposite deal. """
    ticket = link.follower_ticket
    symbol = link.follower_symbol

    if not mt5_symbol_prepare(symbol):
        return False

    if dry_run:
        log(f"[DRY] Would close ticket {ticket} ({symbol}).")
        return True

    # Get the live position
    pos_list = mt5.positions_get(ticket=ticket)
    if not pos_list:
        log(f"[WARN] No MT5 position found for ticket {ticket} (already closed?).", logging.WARNING)
        return False
    pos = pos_list[0]

    # Determine close side and price
    close_side = side_to_reverse_order_type(link.side)
    bid, ask = mt5_current_prices(symbol)
    if bid is None or ask is None or bid <= 0 or ask <= 0:
        log(f"[ERR] No valid price for {symbol}; cannot close.", logging.ERROR)
        return False
    price = ask if close_side == mt5.ORDER_TYPE_BUY else bid

    close_request = {
        "action": mt5.TRADE_ACTION_DEAL,
        "symbol": symbol,
        "volume": pos.volume,
        "type": close_side,
        "position": ticket,
        "price": price,
        "deviation": max_deviation_points,
        "magic": magic_number,
        "comment": f"Close copy of SiRiX {link.master_order_number}",
    }

    result = _order_send_with_fallback(close_request, symbol)
    if result.retcode not in (mt5.TRADE_RETCODE_DONE, mt5.TRADE_RETCODE_DONE_PARTIAL):
        log(f"[ERR] MT5 close failed ticket {ticket}: retcode={result.retcode}", logging.ERROR)
        return False

    log(f"[MT5] Closed ticket {ticket} (master {link.master_order_number}).")
    return True


def mt5_adjust_volume(link: FollowerLink, new_lots: float) -> bool:
    """
    Scale follower position to the requested new_lots.
    Behaviours:
      • If new_lots <= 0: close the position.
      • If greater: add or reduce using market deals.
      • Uses _order_send_with_fallback() to dodge unsupported filling mode errors.
      • Refreshes follower_link.follower_volume from live MT5 after send.
    """
    ticket = link.follower_ticket
    symbol = link.follower_symbol

    if not mt5_symbol_prepare(symbol):
        return False

    # DRY RUN short‑circuit
    if dry_run:
        if new_lots <= 0:
            log(f"[DRY] SCALE->0 so would CLOSE ticket {ticket}")
        else:
            log(f"[DRY] SCALE ticket {ticket} -> {new_lots} lots")
        link.follower_volume = new_lots
        return True

    # Get live position
    pos_list = mt5.positions_get(ticket=ticket)
    if not pos_list:
        log(f"[WARN] No MT5 position found for ticket {ticket}; cannot adjust.", logging.WARNING)
        return False
    pos = pos_list[0]
    current_lots = float(pos.volume)

    # Normalize broker‑acceptable volume
    new_lots = normalize_lot(symbol, new_lots)

    # If target is zero -> close position
    if new_lots <= 0:
        log(f"[adj] Target lots=0. Closing ticket {ticket}.")
        ok = mt5_close_trade(link)
        if ok:
            link.follower_volume = 0.0
        return ok

    diff = new_lots - current_lots
    if abs(diff) < 1e-8:
        # No change
        return True

    bid, ask = mt5_current_prices(symbol)
    if bid is None or ask is None or bid <= 0 or ask <= 0:
        log(f"[ERR] No price for {symbol}; cannot adjust volume.", logging.ERROR)
        return False

    if diff > 0:
        # ADD volume -> same direction
        side = side_to_mt5_order_type(link.side)
        price = ask if side == mt5.ORDER_TYPE_BUY else bid
        req = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": symbol,
            "volume": diff,
            "type": side,
            "price": price,
            "deviation": max_deviation_points,
            "magic": magic_number,
            "comment": f"Volume add copy of SiRiX {link.master_order_number}",
        }
        op_desc = f"+{diff} lots"
    else:
        # REDUCE volume -> opposite direction, with position ID
        reduce_side = side_to_reverse_order_type(link.side)
        price = ask if reduce_side == mt5.ORDER_TYPE_BUY else bid
        req = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": symbol,
            "volume": abs(diff),
            "type": reduce_side,
            "position": ticket,
            "price": price,
            "deviation": max_deviation_points,
            "magic": magic_number,
            "comment": f"Volume reduce copy of SiRiX {link.master_order_number}",
        }
        op_desc = f"-{abs(diff)} lots"

    result = _order_send_with_fallback(req, symbol)
    if result.retcode not in (
        mt5.TRADE_RETCODE_DONE,
        mt5.TRADE_RETCODE_DONE_PARTIAL,
        mt5.TRADE_RETCODE_PLACED,
    ):
        log(f"[ERR] Volume adjust failed ticket {ticket}: retcode={result.retcode}", logging.ERROR)
        return False

    # Refresh from live position (broker may net volumes)
    new_pos = mt5.positions_get(ticket=ticket)
    if new_pos:
        new_vol = float(new_pos[0].volume)
        link.follower_volume = new_vol
        log(f"[MT5] Adjusted volume for {ticket}: {op_desc} -> now {new_vol} lots.")
    else:
        # If position vanished, assume closed
        link.follower_volume = 0.0
        log(f"[MT5] After volume adjust, ticket {ticket} not found (closed?).", logging.WARNING)

    return True


def mt5_update_stops(link: FollowerLink, sl: Optional[float], tp: Optional[float]) -> bool:
    """ Modify SL/TP of an existing follower position. """
    symbol = link.follower_symbol
    ticket = link.follower_ticket

    if not mt5_symbol_prepare(symbol):
        return False

    # No change?
    old_sl, old_tp = link.sl, link.tp
    if sl is not None and old_sl is not None and abs(sl - old_sl) < stop_change_abs_tolerance:
        sl = old_sl
    if tp is not None and old_tp is not None and abs(tp - old_tp) < stop_change_abs_tolerance:
        tp = old_tp

    # If both unchanged and not None -> skip
    if sl == old_sl and tp == old_tp:
        return True

    if dry_run:
        log(f"[DRY] UPDATE SLTP ticket {ticket} SL={sl} TP={tp}")
        link.sl, link.tp = sl, tp
        return True

    add_request = {
        "action": mt5.TRADE_ACTION_SLTP,
        "symbol": symbol,
        "position": ticket,
        "sl": sl if sl is not None else 0.0,
        "tp": tp if tp is not None else 0.0,
        "magic": magic_number,
        "comment": f"Stops sync {link.master_order_number}",
    }
    result = _order_send_with_fallback(add_request, symbol)
    if result.retcode not in (mt5.TRADE_RETCODE_DONE,
                              mt5.TRADE_RETCODE_PLACED,
                              mt5.TRADE_RETCODE_DONE_PARTIAL):
        log(f"[ERR] SL/TP update failed ticket {ticket}: retcode={result.retcode}", logging.ERROR)
        return False

    log(f"[MT5] Updated stops ticket {ticket}: SL={sl} TP={tp}")
    link.sl, link.tp = sl, tp
    return True


# ===================== SiRiX API CALL ========================

def fetch_userstatus_payload(
    user_id: str,
    get_open: bool = True,
    get_pending: bool = True,
    get_closed: bool = True,
    get_monetary: bool = True,
    token: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """
    Execute POST call to SiRiX UserStatus endpoint.
    Returns parsed JSON dict on success, None on failure.
    """
    tk = token if token is not None else sirix_token
    headers = {
        "Authorization": f"Bearer {tk}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    payload = {
        "UserID": str(user_id),
        "GetOpenPositions": bool(get_open),
        "GetPendingPositions": bool(get_pending),
        "GetClosePositions": bool(get_closed),
        "GetMonetaryTransactions": bool(get_monetary),
    }

    try:
        resp = requests.post(sirix_api_url, headers=headers, json=payload, timeout=60)
    except Exception as e:
        log(f"[ERR] Network error: {e}", logging.ERROR)
        return None

    if resp.status_code != 200:
        log(f"[ERR] HTTP {resp.status_code}: {resp.text[:300]}", logging.ERROR)
        return None

    try:
        return resp.json()
    except Exception as e:
        log(f"[ERR] JSON parse error: {e}", logging.ERROR)
        return None


# =================== MASTER SNAPSHOT BUILD ===================

def build_master_snapshot(data: Dict[str, Any]) -> Dict[Any, MasterPosition]:
    """ Convert SiRiX JSON payload into MasterPosition dict keyed by order_number. """
    snap: Dict[Any, MasterPosition] = {}
    for p in data.get("OpenPositions", []) or []:
        order_number = p.get("OrderNumber")
        symbol = p.get("Symbol")
        side = p.get("Side")

        # Guard: If side not recognized, assume BUY.
        if side not in (sirix_buy_value, sirix_sell_value):
            log(f"[WARN] Invalid side '{side}' in SiRiX data (order {order_number}); default BUY.", logging.WARNING)
            side = sirix_buy_value

        amount = p.get("Amount") or 0
        sl, tp = extract_stops(p)
        mp = MasterPosition(
            order_number=order_number,
            symbol=symbol,
            side=side,
            amount=amount,
            open_time=p.get("OpenTime"),
            open_rate=p.get("OpenRate"),
            stop_loss=sl,
            take_profit=tp,
        )
        snap[order_number] = mp
    return snap


def update_master_equity(data: Dict[str, Any]) -> None:
    """ Update global last_sirix_equity from UserData.AccountBalance. Safe if missing. """
    global last_sirix_equity
    try:
        bal = (data.get("UserData") or {}).get("AccountBalance") or {}
        eq = bal.get("Equity")
        if eq is not None:
            last_sirix_equity = float(eq)
    except Exception:
        pass


# ================= PREVIEW TABLE (OPTIONAL) =================

def print_preview_table(master_snap: Dict[Any, MasterPosition]) -> None:
    if not master_snap:
        log("[preview] No open master positions.")
        return

    # DEBUG: Show all master symbols in current snapshot
    log(f"[preview-debug] Master symbols in snapshot: {set(mp.symbol for mp in master_snap.values())}")

    headers = ("MasterOrd", "M.Symbol", "Side", "M.Amt", "F.Symbol", "F.Lots", "Action")
    fmt = "{:<10} {:<9} {:<4} {:>12} {:<12} {:>8} {:<8}"
    print("\n" + fmt.format(*headers))
    print("-" * len(headers))

    for oid, mp in master_snap.items():
        m_sym = mp.symbol
        m_amt = mp.amount
        f_sym = map_symbol(m_sym)
        f_lots = convert_master_amount_to_lots(m_sym, m_amt, follower_symbol=f_sym)
        act = "OPEN" if oid not in link_map else "NONE"
        print(fmt.format(str(oid), m_sym, sirix_dir_char(mp.side),
                         f"{m_amt:.2f}", f_sym, f"{f_lots:.2f}", act))
    print()


# ==================== COPY ENGINE CORE =======================

def stops_changed(old: Optional[float], new: Optional[float]) -> bool:
    # if one is None and other not -> changed
    if (old is None) ^ (new is None):
        return True
    if old is None and new is None:
        return False
    return abs(float(old) - float(new)) > stop_change_abs_tolerance


def open_follower_for_master(mp: MasterPosition) -> None:
    """ Open follower MT5 trade corresponding to a NEW master position. """
    symbol = mp.symbol
    mapped_symbol = map_symbol(symbol)
    lots = convert_master_amount_to_lots(symbol, mp.amount, follower_symbol=mapped_symbol)
    sl, tp = mp.stop_loss, mp.take_profit

    ticket = mt5_open_trade(mp, lots, sl, tp)
    if ticket is None:
        log(f"[WARN] Failed to open follower for master {mp.order_number} ({symbol}).", logging.WARNING)
        return

    link = FollowerLink(
        master_order_number=mp.order_number,
        master_symbol=symbol,
        follower_symbol=mapped_symbol,
        follower_ticket=ticket,
        follower_volume=lots,
        side=mp.side,
        sl=sl,
        tp=tp,
    )

    link_map[mp.order_number] = link


def close_follower_for_master(master_order_number: Any) -> None:
    """ Close follower when master closes (if configured). """
    if not close_on_master_close:
        log(f"[SKIP] Master {master_order_number} closed but auto-close disabled.")
        return
    link = link_map.get(master_order_number)
    if not link:
        log(f"[WARN] No follower link found for closed master {master_order_number}.", logging.WARNING)
        return

    # In dry run, just drop the link
    if dry_run and link.follower_ticket == -1:
        log(f"[DRY] Removing dry-run link for master {master_order_number}.")
        del link_map[master_order_number]
        return

    if mt5_close_trade(link):
        del link_map[master_order_number]


def adjust_follower_for_master(mp: MasterPosition) -> None:
    """ Adjust follower size if master size changed. """
    link = link_map.get(mp.order_number)
    if not link:
        # race: open failed earlier, try open now
        log(f"[INFO] No existing follower for master {mp.order_number}; opening.")
        open_follower_for_master(mp)
        return

    # compute target lots for new master amount
    mapped_symbol = map_symbol(mp.symbol)
    new_lots = convert_master_amount_to_lots(mp.symbol, mp.amount, follower_symbol=mapped_symbol)
    if dry_run:
        log(f"[DRY] SCALE follower {link.follower_ticket} -> {new_lots} lots")
        link.follower_volume = new_lots
        return

    ok = mt5_adjust_volume(link, new_lots)
    if ok and link.follower_volume <= 0:
        # Position flattened during adjust -> drop link
        if link.master_order_number in link_map:
            del link_map[link.master_order_number]


def update_follower_stops_for_master(mp: MasterPosition) -> None:
    link = link_map.get(mp.order_number)
    if not link:
        log(f"[WARN] Stop update: no follower for {mp.order_number}", logging.WARNING)
        return

    sl, tp = mp.stop_loss, mp.take_profit
    if not copy_stops:
        sl = tp = None
    # We don't revalidate here (already validated on open), but we could:
    if validate_stops:
        bid, ask = mt5_current_prices(link.follower_symbol)
        if bid is not None and ask is not None:
            entry = ask if sirix_is_buy(link.side) else bid
            sl, tp = validate_stops_for_mt5(
                link.follower_symbol,
                link.side,
                sirix_dir_01(link.side),
                sl,
                tp,
                entry,
            )

    mt5_update_stops(link, sl, tp)


def process_master_changes(new_snap: Dict[Any, MasterPosition]) -> None:
    """ Compare new master snapshot vs prior and open/close/adjust follower trades. """
    global master_open_snapshot, link_map

    # Detect new + changed
    for order_number, mp in new_snap.items():
        if order_number not in master_open_snapshot:
            # NEW POSITION -> open follower
            open_follower_for_master(mp)
        else:
            # existing: check for size change
            prev = master_open_snapshot[order_number]

            if abs((prev.amount or 0) - (mp.amount or 0)) > 1e-8:
                adjust_follower_for_master(mp)

            # stop change?
            if copy_stops and (stops_changed(prev.stop_loss, mp.stop_loss) or
                               stops_changed(prev.take_profit, mp.take_profit)):
                update_follower_stops_for_master(mp)

    # Detect closed
    closed_ids = set(master_open_snapshot.keys()) - set(new_snap.keys())
    for order_number in closed_ids:
        close_follower_for_master(order_number)

    # Replace snapshot
    master_open_snapshot = new_snap


# ===================== SIGNAL HANDLERS =======================

def _signal_handler(sig, frame):
    global running
    log(f"[signal] Received {sig}; stopping...")
    running = False

signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)


# ======================= MAIN POLL LOOP ======================

def poll_loop():
    """ Main polling loop: fetch master, process changes, repeat until stopped. """
    if not init_mt5():
        log("[fatal] MT5 init failed. Exiting.", logging.ERROR)
        return

    log("[init] Copier running. Ctrl+C to stop.")

    global preview_printed

    while running:
        data = fetch_userstatus_payload(
            master_user_id,
            get_open=req_open_positions,
            get_pending=req_pending_positions,
            get_closed=req_closed_positions,
            get_monetary=req_monetary_transactions,
        )
        if data is None:
            log("[WARN] No data from SiRiX; retrying...", logging.WARNING)
            time.sleep(poll_interval_sec)
            continue

        update_master_equity(data)
        new_snap = build_master_snapshot(data)

        if show_preview_table and not preview_printed:
            print_preview_table(new_snap)
            preview_printed = True
            # show side encoding hint
            print(side_hint_italic)

        process_master_changes(new_snap)

        time.sleep(poll_interval_sec)

    shutdown_mt5()
    log("[done] Copier stopped.")


# =========================== MAIN ============================

if __name__ == "__main__":
    poll_loop()
