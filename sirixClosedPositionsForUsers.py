import os
import time
import requests
import pandas as pd
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from pandas.api.types import DatetimeTZDtype

# ---------------- CONFIGURATION ----------------
BASE_URL = "https://restapi-real3.sirixtrader.com"
TOKEN = "t1_a7xeQOJPnfBzuCncH60yjLFu"

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json"
}

# --- INPUT: CSV with column name exactly: userID ---
INPUT_CSV = r"C:\Users\anish\OneDrive\Desktop\Anish\SIRIX DATA\ClosedPositions_USERS\users.csv"

# --- OUTPUT: folder where per-user XLSX will be saved ---
OUTPUT_DIR = r"C:\Users\anish\OneDrive\Desktop\Anish\SIRIX DATA\ClosedPositions_USERS\output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# --- Date range: pull everything safely ---
START_TIME_UTC_STR = "2022-01-01T00:00:00Z"
# END_DATE = "2025-11-01T00:00:00Z"
END_TIME_UTC_STR = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

# --- Timezones ---
# Treat API times as SERVER local time in Israel (DST aware)
ISRAEL_TZ = ZoneInfo("Asia/Jerusalem")
# Convert to UK local time (DST aware)
UK_TZ = ZoneInfo("Europe/London")


def load_user_ids(csv_path: str) -> list[str]:
    df_users = pd.read_csv(csv_path)
    if "userID" not in df_users.columns:
        raise ValueError(f"CSV must contain a column named 'userID'. Found: {list(df_users.columns)}")

    ids = (
        df_users["userID"]
        .dropna()
        .astype(str)
        .str.strip()
    )
    ids = [x for x in ids if x]
    return sorted(set(ids))


def fetch_closed_positions_for_user(user_id: str, start_time: str, end_time: str) -> list:
    url = f"{BASE_URL}/api/ManagementService/GetClosedPositionsForUser"
    payload = {"userID": str(user_id), "startTime": start_time, "endTime": end_time}

    print(f"[-] Fetching closed positions for userID={user_id} ...")
    try:
        resp = requests.post(url, headers=HEADERS, json=payload, timeout=60)
    except requests.RequestException as e:
        print(f"[ERR] Request failed for userID={user_id}: {e}")
        return []

    if resp.status_code != 200:
        print(f"[ERR] userID={user_id} | HTTP {resp.status_code}: {resp.text}")
        return []

    data = resp.json() if resp.content else {}
    rows = data.get("ClosedPositions", []) or []
    print(f"[DONE] userID={user_id} | positions={len(rows)}")
    return rows


def apply_time_conversion_israel_to_uk(df: pd.DataFrame) -> pd.DataFrame:
    """
    Server time is the ORIGINAL pulled time.
    We treat API OpenTime/CloseTime as ISRAEL local time (DST-aware),
    then convert to UK (GMT/BST-aware), exactly like your working script logic.
    """
    if df.empty:
        return df

    # Ensure columns exist
    for col in ["OpenTime", "CloseTime", "ActionType"]:
        if col not in df.columns:
            df[col] = pd.NA

    # --- Action mapping next to ActionType ---
    df["Action"] = df["ActionType"].map({0: "BUY", 1: "SELL"}).fillna("UNKNOWN")

    # --- Parse times WITHOUT forcing utc=True (important) ---
    open_dt = pd.to_datetime(df["OpenTime"], errors="coerce", utc=False)
    close_dt = pd.to_datetime(df["CloseTime"], errors="coerce", utc=False)

    # If API strings include timezone (e.g. ...Z), open_dt may already be tz-aware.
    # Your desired behaviour: treat "original pulled time" as SERVER Israel local.
    # So:
    # - if tz-naive -> localize Israel
    # - if tz-aware -> convert to Israel (to preserve "server local view")
    def to_israel(series: pd.Series) -> pd.Series:
        if isinstance(series.dtype, DatetimeTZDtype):
            return series.dt.tz_convert(ISRAEL_TZ)
        # tz-naive
        return series.dt.tz_localize(ISRAEL_TZ, ambiguous="NaT", nonexistent="shift_forward")

    open_server = to_israel(open_dt)
    close_server = to_israel(close_dt)

    # Drop rows where OpenTime can't be resolved (rare DST edge cases)
    mask_valid = open_server.notna()
    df = df.loc[mask_valid].reset_index(drop=True)
    open_server = open_server[mask_valid].reset_index(drop=True)
    close_server = close_server[mask_valid].reset_index(drop=True)

    # Create the fields you asked for
    df["OpenTime_server"] = open_server
    df["OpenTime_uk"] = df["OpenTime_server"].dt.tz_convert(UK_TZ)

    df["CloseTime_server"] = close_server
    df["CloseTime_uk"] = df["CloseTime_server"].dt.tz_convert(UK_TZ)

    # Excel-friendly: drop tz info (keep local clock values)
    for col in ["OpenTime_server", "OpenTime_uk", "CloseTime_server", "CloseTime_uk"]:
        if col in df.columns and isinstance(df[col].dtype, DatetimeTZDtype):
            df[col] = df[col].dt.tz_localize(None)

    return df


def reorder_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    preferred = []

    # Put key identifiers first if present
    for c in ["UserID", "OrderID", "InstrumentName", "Amount", "AmountLots", "FullAmount"]:
        if c in df.columns:
            preferred.append(c)

    # ActionType then Action next to it
    for c in ["ActionType", "Action"]:
        if c in df.columns:
            preferred.append(c)

    # Times grouped
    for c in ["OpenTime_server", "OpenTime_uk", "OpenRate", "CloseTime_server", "CloseTime_uk", "CloseRate"]:
        if c in df.columns:
            preferred.append(c)

    # Add the rest
    remaining = [c for c in df.columns if c not in preferred]
    return df[preferred + remaining]


def save_user_excel(user_id: str, positions: list):
    if not positions:
        print(f"[INFO] userID={user_id} has no closed positions. Skipping file.")
        return

    df = pd.DataFrame(positions)
    df = apply_time_conversion_israel_to_uk(df)
    df = reorder_columns(df)

    out_path = os.path.join(OUTPUT_DIR, f"{user_id}_closedTrades.xlsx")

    with pd.ExcelWriter(out_path, engine="xlsxwriter") as writer:
        df.to_excel(writer, sheet_name="All Data", index=False)

        workbook = writer.book
        worksheet = writer.sheets["All Data"]

        # Freeze header
        worksheet.freeze_panes(1, 0)

        # Auto column width (capped)
        for i, col in enumerate(df.columns):
            col_series = df[col].astype(str)
            max_len = max(col_series.map(len).max() if len(col_series) else 0, len(col)) + 2
            worksheet.set_column(i, i, min(max_len, 60))

        # ===============================
        # BUY / SELL colour formatting
        # ===============================
        if "Action" in df.columns:
            action_col_idx = df.columns.get_loc("Action")

            format_buy = workbook.add_format({
                "bg_color": "#C6EFCE",
                "font_color": "#006100"
            })
            format_sell = workbook.add_format({
                "bg_color": "#FFC7CE",
                "font_color": "#9C0006"
            })

            worksheet.conditional_format(
                1, action_col_idx, len(df), action_col_idx,
                {
                    "type": "text",
                    "criteria": "containing",
                    "value": "BUY",
                    "format": format_buy
                }
            )

            worksheet.conditional_format(
                1, action_col_idx, len(df), action_col_idx,
                {
                    "type": "text",
                    "criteria": "containing",
                    "value": "SELL",
                    "format": format_sell
                }
            )

    print(f"[SAVED] userID={user_id} -> {out_path}")


def main():
    print("[START] Closed trades export (per user)")
    print(f"[-] Range: {START_TIME_UTC_STR} -> {END_TIME_UTC_STR}")
    print(f"[-] Input CSV: {INPUT_CSV}")
    print(f"[-] Output dir: {OUTPUT_DIR}\n")

    start = time.time()
    user_ids = load_user_ids(INPUT_CSV)
    print(f"[INFO] Users loaded: {len(user_ids)}\n")

    for idx, user_id in enumerate(user_ids, start=1):
        print(f"=== ({idx}/{len(user_ids)}) userID={user_id} ===")
        rows = fetch_closed_positions_for_user(user_id, START_TIME_UTC_STR, END_TIME_UTC_STR)
        save_user_excel(user_id, rows)
        print()

    print(f"[DONE] Completed in {time.time() - start:.2f}s")


if __name__ == "__main__":
    main()
