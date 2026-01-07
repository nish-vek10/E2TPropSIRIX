import os
import time
import requests
import pandas as pd
from datetime import datetime, timezone
from pandas.api.types import DatetimeTZDtype

# =========================
# CONFIG
# =========================
BASE_URL = "https://restapi-real3.sirixtrader.com"
TOKEN = "t1_a7xeQOJPnfBzuCncH60yjLFu"

TEST_USER_ID = None  # UserID for preview, or set None

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json",
    "Accept": "application/json",
}

GROUPS = ["Audition", "Funded", "Purchases"]

START_DATE = "2010-01-01T00:00:00Z"
END_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

OUTPUT_DIR = r"C:\Users\anish\OneDrive\Desktop\Anish\SIRIX DATA\ClosedPositions_AllUsers_PerUser"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Endpoints
GROUPS_CLOSED_URL = f"{BASE_URL}/api/ManagementService/GetClosedPositionsForGroups"
ALL_USERS_URL = f"{BASE_URL}/api/ManagementService/GetAllUsers"
USER_CLOSED_URL = f"{BASE_URL}/api/ManagementService/GetClosedPositionsForUser"
USER_TXN_URL = f"{BASE_URL}/api/UserStatus/GetUserTransactions"

# Your required output columns (exact order)
OUT_COLS = [
    "UserID",
    "InitialBalance",
    "OrderID",
    "TradeNo",
    "InstrumentName",
    "AmountLots",
    "Action",
    "OpenTime_server",
    "CloseTime_server",
    "OpenRate",
    "CloseRate",
    "StopLoss",
    "TakeProfit",
    "ClosedProfit",
    "AccountBalance",
    "NetPct",
    "Status",
]


# =========================
# HELPERS
# =========================

def fmt_mm_ss(seconds: float) -> str:
    minutes = int(seconds // 60)
    secs = int(seconds % 60)
    return f"{minutes:02d}:{secs:02d}"


def safe_float(x):
    try:
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return None
        return float(x)
    except Exception:
        return None


def safe_int(x):
    try:
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return None
        return int(float(x))
    except Exception:
        return None


def parse_dt_clean(v):
    """
    Parse a Sirix datetime string and return tz-naive datetime floored to seconds.
    Output will display in Excel as 'YYYY-MM-DD HH:MM:SS' with formatting.
    """
    if not v:
        return pd.NaT
    dt = pd.to_datetime(v, errors="coerce", utc=False)
    if isinstance(getattr(dt, "dtype", None), DatetimeTZDtype):
        dt = dt.dt.tz_localize(None)
    # If scalar Timestamp with tzinfo:
    try:
        if getattr(dt, "tzinfo", None) is not None:
            dt = dt.tz_localize(None)
    except Exception:
        pass
    try:
        return dt.floor("s")
    except Exception:
        return dt


def fetch_all_users() -> list[dict]:
    print("[-] Fetching ALL users...")
    r = requests.post(ALL_USERS_URL, headers=HEADERS, json={}, timeout=90)
    if r.status_code != 200:
        print(f"[ERR] GetAllUsers HTTP {r.status_code}: {r.text}")
        return []
    users = (r.json() or {}).get("Users", []) or []
    print(f"[DONE] Users fetched: {len(users)}")
    return users


def fetch_closed_positions_for_user(user_id: int, start_time: str, end_time: str) -> list[dict]:
    payload = {"userID": str(user_id), "startTime": start_time, "endTime": end_time}
    try:
        r = requests.post(USER_CLOSED_URL, headers=HEADERS, json=payload, timeout=90)
    except requests.RequestException as e:
        print(f"[ERR] ClosedPositions user request failed userID={user_id}: {e}")
        return []
    if r.status_code != 200:
        print(f"[ERR] ClosedPositions user HTTP {r.status_code} userID={user_id}: {r.text[:200]}")
        return []
    data = r.json() if r.content else {}
    return (data.get("ClosedPositions", []) or [])


def fetch_closed_positions_for_groups(groups, start_time, end_time) -> list[dict]:
    payload = {"groups": groups, "startTime": start_time, "endTime": end_time}
    print("[-] Fetching closed positions for groups...")
    print(f"    Groups: {groups}")
    print(f"    Range : {start_time} -> {end_time}")

    try:
        r = requests.post(GROUPS_CLOSED_URL, headers=HEADERS, json=payload, timeout=90)
    except requests.RequestException as e:
        print(f"[ERR] Groups request failed: {e}")
        return []

    if r.status_code != 200:
        print(f"[ERR] Groups HTTP {r.status_code}: {r.text}")
        return []

    data = r.json() if r.content else {}
    rows = data.get("ClosedPositions", []) or []
    print(f"[DONE] Closed positions fetched: {len(rows)}")
    return rows


def fetch_user_transactions(user_id: int) -> list[dict]:
    payload = {
        "UserID": str(user_id),
        "GetOpenPositions": False,
        "GetPendingPositions": False,
        "GetClosePositions": False,
        "GetMonetaryTransactions": True,
    }
    try:
        r = requests.post(USER_TXN_URL, headers=HEADERS, json=payload, timeout=40)
    except requests.RequestException as e:
        print(f"[ERR] User txns request failed userID={user_id}: {e}")
        return []

    if r.status_code != 200:
        print(f"[ERR] User txns HTTP {r.status_code} userID={user_id}: {r.text}")
        return []

    data = r.json() if r.content else {}
    return data.get("MonetaryTransactions") or []


def extract_initial_balance_and_zero_balance(txns: list[dict]):
    """
    Returns:
      initial_balance (float|None)
      zero_balance_amount (float|None)   -> positive absolute amount
      zero_balance_time (datetime|NaT)
      blown_up (bool)
    """
    initial_balance = None
    zb_amount = None
    zb_time = pd.NaT

    for t in txns:
        comment = str(t.get("Comment", "")).strip().lower()
        amt = safe_float(t.get("Amount"))
        t_time = parse_dt_clean(t.get("CreateDate") or t.get("CreatedAt") or t.get("Date") or t.get("Time"))

        if comment.startswith("initial balance") and amt is not None and initial_balance is None:
            initial_balance = float(abs(amt))

        if "zero balance" in comment and amt is not None and zb_amount is None:
            zb_amount = float(abs(amt))
            zb_time = t_time

    blown_up = zb_amount is not None
    return initial_balance, zb_amount, zb_time, blown_up


def build_user_df(user_id: int, user_rows: list[dict], txns: list[dict]) -> pd.DataFrame:
    if not user_rows:
        return pd.DataFrame(columns=OUT_COLS)

    df = pd.DataFrame(user_rows)

    # Force numeric IDs so Excel writes as numbers
    df["UserID"] = pd.to_numeric(df.get("UserID"), errors="coerce").astype("Int64")
    df["OrderID"] = pd.to_numeric(df.get("OrderID"), errors="coerce").astype("Int64")

    # Action mapping
    if "ActionType" not in df.columns:
        df["ActionType"] = pd.NA
    df["Action"] = df["ActionType"].map({0: "BUY", 1: "SELL"}).fillna("UNKNOWN")

    # Clean time fields (NO tz conversion, NO milliseconds)
    df["OpenTime_server"] = df.get("OpenTime").apply(parse_dt_clean) if "OpenTime" in df.columns else pd.NaT
    df["CloseTime_server"] = df.get("CloseTime").apply(parse_dt_clean) if "CloseTime" in df.columns else pd.NaT

    # Profit rename
    if "ProfitInAccountCurrency" not in df.columns:
        df["ProfitInAccountCurrency"] = pd.NA
    df["ClosedProfit"] = pd.to_numeric(df["ProfitInAccountCurrency"], errors="coerce").fillna(0.0)

    # Extract initial + zero balance
    initial_balance, zb_amount, zb_time, blown_up = extract_initial_balance_and_zero_balance(txns)
    ib = safe_float(initial_balance)

    df["InitialBalance"] = ib

    # Sort by earliest OpenTime FIRST (your requirement)
    # Tie-breaker: OrderID
    sort_cols = ["OpenTime_server"]
    if "OrderID" in df.columns:
        sort_cols.append("OrderID")

    df = df.sort_values(by=sort_cols, ascending=True).reset_index(drop=True)

    # TradeNo
    df["TradeNo"] = range(1, len(df) + 1)

    # Running balance + NetPct
    if ib is None or ib == 0:
        df["AccountBalance"] = pd.NA
        df["NetPct"] = pd.NA
    else:
        df["AccountBalance"] = ib + df["ClosedProfit"].cumsum()
        df["NetPct"] = (((df["AccountBalance"] / ib) - 1.0) * 100.0).round(2)

    # Status (default Active)
    df["Status"] = "Active"

    # If blown up: add final synthetic row that "locks" AccountBalance to Zero Balance amount
    # and sets status to BlownUp
    if blown_up:
        # If we have an IB we can compute a "forced" NetPct from the locked balance
        forced_balance = zb_amount
        forced_netpct = None
        if ib is not None and ib != 0 and forced_balance is not None:
            forced_netpct = round(((forced_balance / ib) - 1.0) * 100.0, 2)

        # Append BlowUp row WITHOUT concat (avoids FutureWarning)
        new_idx = len(df)

        df.loc[new_idx, "UserID"] = user_id
        df.loc[new_idx, "InitialBalance"] = ib
        df.loc[new_idx, "OrderID"] = pd.NA
        df.loc[new_idx, "TradeNo"] = pd.NA
        df.loc[new_idx, "InstrumentName"] = "Zero-Balance"
        df.loc[new_idx, "AmountLots"] = pd.NA
        df.loc[new_idx, "Action"] = pd.NA
        df.loc[new_idx, "OpenTime_server"] = pd.NaT
        df.loc[new_idx, "CloseTime_server"] = zb_time
        df.loc[new_idx, "OpenRate"] = pd.NA
        df.loc[new_idx, "CloseRate"] = pd.NA
        df.loc[new_idx, "StopLoss"] = pd.NA
        df.loc[new_idx, "TakeProfit"] = pd.NA
        df.loc[new_idx, "ClosedProfit"] = 0.0
        df.loc[new_idx, "AccountBalance"] = forced_balance
        df.loc[new_idx, "NetPct"] = forced_netpct
        df.loc[new_idx, "Status"] = "BlownUp"

        # Optional: set ALL rows to Active until last which is BlownUp (already true)
        df.loc[df.index[:-1], "Status"] = "Active"
        df.loc[df.index[-1], "Status"] = "BlownUp"

    # Ensure required columns exist and order them
    for c in OUT_COLS:
        if c not in df.columns:
            df[c] = pd.NA
    df = df[OUT_COLS]

    # Sort finally by TradeNo (so Excel is always ordered)
    df = df.sort_values(by=["TradeNo"], ascending=True).reset_index(drop=True)

    return df


def save_user_excel(user_id: int, df: pd.DataFrame):
    if df.empty:
        print(f"[INFO] userID={user_id}: no rows -> skip")
        return

    out_path = os.path.join(OUTPUT_DIR, f"{user_id}_closedTrades.xlsx")

    with pd.ExcelWriter(out_path, engine="xlsxwriter") as writer:
        df.to_excel(writer, sheet_name="Trades", index=False)

        workbook = writer.book
        ws = writer.sheets["Trades"]

        # Header: light gray
        header_fmt = workbook.add_format({"bold": True, "bg_color": "#D9D9D9", "border": 1})
        ws.set_row(0, None, header_fmt)

        # Freeze header
        ws.freeze_panes(1, 0)

        # Date format for Excel display
        dt_fmt = workbook.add_format({"num_format": "yyyy-mm-dd hh:mm:ss"})

        # Profit formats
        format_bold = workbook.add_format({"bold": True})
        format_buy = workbook.add_format({"bg_color": "#C6EFCE", "font_color": "#006100"})
        format_sell = workbook.add_format({"bg_color": "#FFC7CE", "font_color": "#9C0006"})
        format_profit_pos = workbook.add_format({"font_color": "#0000FF"})  # blue
        format_profit_neg = workbook.add_format({"font_color": "#9C0006"})  # red

        # Auto column width (capped)
        for i, col in enumerate(df.columns):
            ser = df[col].astype(str)
            max_len = max(ser.map(len).max() if len(ser) else 0, len(col)) + 2
            ws.set_column(i, i, min(max_len, 60))

        # Apply datetime format to time columns
        for colname in ("OpenTime_server", "CloseTime_server"):
            if colname in df.columns:
                cidx = df.columns.get_loc(colname)
                ws.set_column(cidx, cidx, 20, dt_fmt)

        # Conditional formatting for Action
        if "Action" in df.columns:
            action_col = df.columns.get_loc("Action")
            ws.conditional_format(
                1, action_col, len(df), action_col,
                {"type": "text", "criteria": "containing", "value": "BUY", "format": format_buy}
            )
            ws.conditional_format(
                1, action_col, len(df), action_col,
                {"type": "text", "criteria": "containing", "value": "SELL", "format": format_sell}
            )

        # Conditional formatting for ClosedProfit
        if "ClosedProfit" in df.columns:
            prof_col = df.columns.get_loc("ClosedProfit")
            ws.conditional_format(
                1, prof_col, len(df), prof_col,
                {"type": "cell", "criteria": ">", "value": 0, "format": format_profit_pos}
            )
            ws.conditional_format(
                1, prof_col, len(df), prof_col,
                {"type": "cell", "criteria": "<", "value": 0, "format": format_profit_neg}
            )

        # Bold formatting for Zero-Balance instrument row
        if "InstrumentName" in df.columns:
            instr_col = df.columns.get_loc("InstrumentName")

            ws.conditional_format(
                1, instr_col, len(df), instr_col,
                {
                    "type": "text",
                    "criteria": "containing",
                    "value": "Zero-Balance",
                    "format": format_bold
                }
            )

    print(f"[SAVED] userID={user_id} -> {out_path}")


def main():
    print("[START] Closed trades export (Groups -> Per User)")
    print(f"[-] Groups: {GROUPS}")
    print(f"[-] Range : {START_DATE} -> {END_DATE}")
    print(f"[-] Output: {OUTPUT_DIR}\n")

    start_ts = time.time()

    # 1) Fetch ALL users (this is the driver)
    users = fetch_all_users()
    if not users:
        print("[DONE] No users returned from GetAllUsers.")
        return

    df_users = pd.DataFrame(users)

    # 2) Filter out GroupName containing "free trial" or "test"
    if "GroupName" in df_users.columns:
        gn = df_users["GroupName"].astype(str).str.lower()
        df_users = df_users[~gn.str.contains(r"free trial|test", regex=True, na=False)].copy()

    # 3) Keep only valid numeric UserID
    df_users["UserID"] = pd.to_numeric(df_users.get("UserID"), errors="coerce").astype("Int64")
    df_users = df_users[df_users["UserID"].notna()].copy()

    user_ids = sorted(df_users["UserID"].unique().tolist())
    print(f"[INFO] Users after GroupName filter: {len(user_ids)}\n")

    # Cache txns per user
    txns_cache: dict[int, list[dict]] = {}

    # --- OPTIONAL: preview only one user ---
    if TEST_USER_ID is not None:
        if TEST_USER_ID not in user_ids:
            print(f"[TEST] TEST_USER_ID={TEST_USER_ID} not found in GetAllUsers list (after filter).")
            return
        user_ids = [TEST_USER_ID]
        print(f"[TEST] Running in single-user mode: {TEST_USER_ID}\n")

    for idx, uid in enumerate(user_ids, start=1):

        elapsed_so_far = time.time() - start_ts
        print(
            f"=== ({idx}/{len(user_ids)}) userID={uid} | "
            f"elapsed={fmt_mm_ss(elapsed_so_far)} ==="
        )

        if uid not in txns_cache:
            txns = fetch_user_transactions(uid)
            txns_cache[uid] = txns
        else:
            txns = txns_cache[uid]

        user_rows = fetch_closed_positions_for_user(uid, START_DATE, END_DATE)
        df_user = build_user_df(uid, user_rows, txns)
        save_user_excel(uid, df_user)
        print()

        time.sleep(0.25)

    elapsed = time.time() - start_ts
    print(f"[DONE] Completed in {fmt_mm_ss(elapsed)} (MM:SS)")


if __name__ == "__main__":
    main()
