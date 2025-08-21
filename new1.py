"""
SiRiX Trader Full Data Pull via GET
==================================

Retrieves all available trader info from the SiRiX endpoint:
    GET /api/UserStatus/GetUserTransactions

We send query params (userId, page, pageSize, optional date range).
If the server ignores lowercase `userId`, we retry with `UserId`.

Printed sections:
- User Data (profile, balances, margin)
- Open Positions
- Pending Orders
- Closed Positions (history returned in payload, not auto-paged unless API supports it)
- Monetary Transactions

NOTE: Some SiRiX deployments only return limited history here. If ClosedPositions
remains empty but you expect trades, we may need an account-level or reports endpoint.
"""

import requests
from datetime import datetime
from typing import Any, Dict, Optional, List, Tuple

# ================== CONFIG ==================
API_ROOT = "https://restapi-real3.sirixtrader.com"
TOKEN = "t1_a7xeQOJPnfBzuCncH60yjLFu"  # <-- move to env var in production
USER_ID = 117614  # <-- trader to query
PAGE = 1  # some deployments ignore; safe to include
PAGE_SIZE = 200  # server may cap; safe to include
DATE_FROM = None  # e.g. "2025-01-01"
DATE_TO = None  # e.g. "2025-07-17"
# ===========================================


ENDPOINT_PATH = "/api/UserStatus/GetUserTransactions"


# ------------------------------------------------------------------
# Low-level GET helper
# ------------------------------------------------------------------
def _do_get(params: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], int, str]:
    """Perform GET call and return (json_dict_or_None, status_code, text)."""
    url = API_ROOT.rstrip("/") + ENDPOINT_PATH
    headers = {
        "Authorization": f"Bearer {TOKEN}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    try:
        resp = requests.get(url, headers=headers, params=params, timeout=60)
    except Exception as e:
        return None, 0, f"Network error: {e}"

    if resp.status_code != 200:
        return None, resp.status_code, resp.text

    try:
        return resp.json(), resp.status_code, resp.text
    except Exception as e:
        return None, resp.status_code, f"JSON parse error: {e}"


# ------------------------------------------------------------------
# Public fetch — tries param casing variants
# ------------------------------------------------------------------
def fetch_user_status_get(user_id: int,
                          page: int = PAGE,
                          page_size: int = PAGE_SIZE,
                          date_from: Optional[str] = DATE_FROM,
                          date_to: Optional[str] = DATE_TO) -> Optional[Dict[str, Any]]:
    """
    Call the endpoint using GET.
    Tries `userId` first; if that fails (non-200 or empty), retries with `UserId`.
    Includes optional page, pageSize, from, to params if provided.
    """
    # Build base params
    params = {}
    if date_from: params["from"] = date_from
    if date_to:   params["to"] = date_to
    if page is not None: params["page"] = page
    if page_size is not None: params["pageSize"] = page_size

    # --- Attempt 1: lower camel (common REST style) ---
    params1 = {"userId": user_id, **params}
    data, code, txt = _do_get(params1)
    if data is not None:
        return data

    # --- Attempt 2: Pascal / API-style ---
    params2 = {"UserId": user_id, **params}
    data, code, txt = _do_get(params2)
    if data is not None:
        return data

    # Both failed
    print(f"[!] GET failed. Last status: {code}")
    print(txt[:500])
    return None


# ------------------------------------------------------------------
# Formatting helpers
# ------------------------------------------------------------------
def _fmt_num(v: Any, nd: int = 2) -> Any:
    if isinstance(v, (int, float)):
        return f"{v:,.{nd}f}"
    return v


def _fmt_dt(ts: Any) -> Any:
    if not ts:
        return ts
    try:
        dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return ts


# ------------------------------------------------------------------
# Print sections
# ------------------------------------------------------------------
def print_user_data(data: Dict[str, Any]) -> None:
    u = data.get("UserData", {})
    det = u.get("UserDetails", {})
    bal = u.get("AccountBalance", {})
    marg = u.get("MarginRequirements", {})
    grp = u.get("GroupInfo", {})
    term = u.get("TradingTermInfo", {})

    print("\n=== USER DATA ===")
    print(f"UserID             : {u.get('UserID')}")
    print(f"FullName           : {det.get('FullName')}")
    print(f"IDNumber           : {det.get('IDNumber')}")
    print(f"Email              : {det.get('Email')}")
    print(f"Phone              : {det.get('Phone')}")
    print(f"Country            : {det.get('Country')}")
    print(f"State              : {det.get('State')}")
    print(f"City               : {det.get('City')}")
    print(f"Address            : {det.get('Address')}")
    print(f"ZipCode            : {det.get('ZipCode')}")
    print(f"CreationTime       : {_fmt_dt(det.get('CreationTime'))}")
    print(f"Source             : {det.get('Source')}")
    print(f"Comment            : {det.get('Comment')}")

    print("\n--- Group Info ---")
    print(f"GroupId            : {grp.get('GroupId')}")
    print(f"GroupName          : {grp.get('GroupName')}")

    print("\n--- Trading Term ---")
    print(f"TradingTermId      : {term.get('TradingTermId')}")
    print(f"TradingTermName    : {term.get('TradingTermName')}")

    print("\n--- Trading State / Tradability ---")
    print(f"TradingState       : {u.get('TradingState')}")
    print(f"Tradability        : {u.get('Tradability')}")

    print("\n--- Margin Requirements ---")
    print(f"Leverage           : {marg.get('Leverage')}")
    print(f"MarginCoefficient  : {marg.get('MarginCoefficient')}")
    print(f"UseAcctMarginReq   : {marg.get('UseAccountMarginRequirements')}")
    print(f"AcctHedgingRiskMode: {marg.get('AccountHedgingRiskMode')}")

    print("\n--- Account Balance ---")
    print(f"BalanceUserID      : {bal.get('UserID')}")
    print(f"Balance            : {_fmt_num(bal.get('Balance'))}")
    print(f"Equity             : {_fmt_num(bal.get('Equity'))}")
    print(f"Margin             : {_fmt_num(bal.get('Margin'))}")
    print(f"Credit             : {_fmt_num(bal.get('Credit'))}")
    print(f"OpenPnL            : {_fmt_num(bal.get('OpenPnL'))}")
    print(f"MarginLevel        : {_fmt_num(bal.get('MarginLevel'))}")
    print(f"FreeMargin         : {_fmt_num(bal.get('FreeMargin'))}")
    print(f"Currency           : {u.get('Currency')}")

    labels = u.get("Labels") or []
    if labels:
        print("\n--- Labels ---")
        for L in labels:
            print(f"LabelId {L.get('LabelId')} : {L.get('LabelName')}")


def print_open_positions(data: Dict[str, Any]) -> None:
    pos_list = data.get("OpenPositions") or []
    print("\n=== OPEN POSITIONS ===")
    if not pos_list:
        print("None.")
        return
    for p in pos_list:
        print(f"{_fmt_dt(p.get('OpenTime'))} | {p.get('Symbol')} | "
              f"Side: {p.get('Side')} | Amt: {p.get('Amount')} | "
              f"Open: {p.get('OpenRate')} | Curr: {p.get('CurrentRate')} | "
              f"SL: {p.get('StopLoss')} | TP: {p.get('TakeProfit')} | "
              f"Swap: {p.get('Swap')} | Comm: {p.get('Commission')} | "
              f"PnL: {p.get('Profit')} | TotPnL: {p.get('TotalProfit')} | "
              f"Comment: {p.get('Comment')}")


def print_pending_orders(data: Dict[str, Any]) -> None:
    ord_list = data.get("PendingOrders") or []
    print("\n=== PENDING ORDERS ===")
    if not ord_list:
        print("None.")
        return
    for o in ord_list:
        print(f"{_fmt_dt(o.get('CreationTime'))} | {o.get('Symbol')} | "
              f"Type: {o.get('PendingOrderType')} | Amt: {o.get('Amount')} | "
              f"Price: {o.get('Price')} | SL: {o.get('StopLoss')} | TP: {o.get('TakeProfit')} | "
              f"Expire: {_fmt_dt(o.get('ExpirationTime'))} | Comment: {o.get('Comment')}")


def print_closed_positions(data: Dict[str, Any]) -> None:
    cp_list = data.get("ClosedPositions") or []
    print("\n=== CLOSED POSITIONS ===")
    if not cp_list:
        print("None.")
        return

    # Newest first
    cp_list = sorted(cp_list, key=lambda t: t.get("CloseTime"), reverse=True)

    for c in cp_list:
        print(f"{_fmt_dt(c.get('CloseTime'))} | {c.get('Symbol')} | "
              f"Side: {c.get('Side')} | Amt: {c.get('Amount')} | "
              f"Open: {_fmt_dt(c.get('OpenTime'))} @{c.get('OpenRate')} | "
              f"Close: {_fmt_dt(c.get('CloseTime'))} @{c.get('CloseRate')} | "
              f"Swap: {c.get('Swap')} | Comm: {c.get('Commission')} | "
              f"PnL: {c.get('Profit')} | TotPnL: {c.get('TotalProfit')} | "
              f"Comment: {c.get('Comment')}")
    print(f"\nTotal Closed Trades: {len(cp_list)}")


def print_monetary_transactions(data: Dict[str, Any]) -> None:
    tx_list = data.get("MonetaryTransactions") or []
    print("\n=== MONETARY TRANSACTIONS ===")
    if not tx_list:
        print("None.")
        return

    # Newest first
    tx_list = sorted(tx_list, key=lambda t: t.get("Time"), reverse=True)

    for t in tx_list:
        print(f"{_fmt_dt(t.get('Time'))} | Order#: {t.get('OrderNumber')} | "
              f"Type: {t.get('Type')} | Amt: {t.get('Amount')} | Comment: {t.get('Comment')}")
    print(f"\nTotal Monetary Transactions: {len(tx_list)}")


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------
if __name__ == "__main__":
    data = fetch_user_status_get(USER_ID)

    if not data:
        raise SystemExit("[!] No data returned from GET call.")

    # Quick debug
    print("\n[DEBUG] Keys:", list(data.keys()))
    print("[DEBUG] Counts:",
          "Closed:", len(data.get("ClosedPositions", [])),
          "| Open:", len(data.get("OpenPositions", [])),
          "| Pending:", len(data.get("PendingOrders", [])),
          "| Tx:", len(data.get("MonetaryTransactions", [])))

    # Print all sections
    print_user_data(data)
    print_open_positions(data)
    print_pending_orders(data)
    print_closed_positions(data)
    print_monetary_transactions(data)
