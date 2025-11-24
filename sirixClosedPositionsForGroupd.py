import os
import requests
import pandas as pd
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo  # for Israel + Europe/London DST handling
import time
from pandas.api.types import DatetimeTZDtype

# ---------------- CONFIGURATION ----------------
BASE_URL = "https://restapi-real3.sirixtrader.com"
TOKEN = "t1_a7xeQOJPnfBzuCncH60yjLFu"
TRADER_ID = "188320"  # Not used for this endpoint but kept for reference

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json"
}

# --- Directory to save report ---
SAVE_DIR = r"C:\Users\anish\OneDrive\Desktop\Anish\SIRIX DATA\ByGroupsNEW"
os.makedirs(SAVE_DIR, exist_ok=True)

# --- Timezones ---
# SiRiX server time: Israel local time (IST/IDT, DST-aware)
ISRAEL_TZ = ZoneInfo("Asia/Jerusalem")

# UK time with automatic GMT/BST handling
UK_TZ = ZoneInfo("Europe/London")

# Date range (you are currently expressing these as UTC with 'Z')
# We'll convert these to Israel time for filtering.
START_DATE = "2025-11-01T00:00:00Z"
# END_DATE   = "2025-11-01T00:00:00Z"
END_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

# --- File name based on START_DATE and END_DATE (e.g. 010425-300425) ---
def format_date_label(date_str: str) -> str:
    """
    Convert ISO string like '2025-04-01T00:00:00Z' → '010425'.
    Uses only the date part.
    """
    dt = datetime.strptime(date_str[:10], "%Y-%m-%d")
    return dt.strftime("%d%m%y")

# Start label (e.g. 010425)
start_label = format_date_label(START_DATE)

# End label: we use END_DATE minus 1 day so that
# 2025-06-01 becomes 31-05-25 → '310525'
end_dt_for_label = datetime.strptime(END_DATE[:10], "%Y-%m-%d") - timedelta(days=1)
end_label = end_dt_for_label.strftime("%d%m%y")

OUTPUT_FILE = os.path.join(
    SAVE_DIR,
    f"closed_positions_report_{start_label}-{end_label}.xlsx"
)

# --- Groups to pull ---
GROUPS = ["Audition", "Funded", "Purchases"]


def fetch_closed_positions_for_groups(groups, start_time, end_time):
    """
    Fetch closed positions for the given groups and time range
    using /api/ManagementService/GetClosedPositionsForGroups.
    """
    url = f"{BASE_URL}/api/ManagementService/GetClosedPositionsForGroups"

    payload = {
        "groups": groups,
        "startTime": start_time,
        "endTime": end_time
    }

    print("[-] Sending request to fetch closed positions for groups...")
    print(f"    Groups: {groups}")
    response = requests.post(url, headers=HEADERS, json=payload)

    if response.status_code == 200:
        data = response.json()
        closed_positions = data.get("ClosedPositions", [])
        print(f"[DONE] Successfully fetched {len(closed_positions)} closed positions.")
        return closed_positions
    else:
        print(f"[ERR] Error {response.status_code}: {response.text}")
        return []


def create_excel_report(closed_positions):
    """
    Save all raw and filtered data to an Excel file.
    Adds color coding for BUY/SELL and Profit cells.
    """
    if not closed_positions:
        print("[ERR] No data to save.")
        return

    print("[-] Preparing data for Excel...")

    # --- Sheet 1: Full raw data ---
    df_full = pd.DataFrame(closed_positions)

    # --- Parse times as Israel server local, then derive UK (GMT/BST) ---
    open_raw = df_full.get("OpenTime")
    close_raw = df_full.get("CloseTime")

    # Parse to datetime (no tz yet)
    open_dt = pd.to_datetime(open_raw, errors="coerce", utc=False)
    close_dt = pd.to_datetime(close_raw, errors="coerce", utc=False)

    # Attach Israel timezone (IST/IDT).
    # For DST edge cases:
    # - ambiguous='NaT'      -> any truly ambiguous wall times become NaT
    # - nonexistent='shift_forward' -> spring-forward gaps are nudged to next valid time
    open_server = open_dt.dt.tz_localize(
        ISRAEL_TZ,
        ambiguous="NaT",
        nonexistent="shift_forward"
    )

    close_server = close_dt.dt.tz_localize(
        ISRAEL_TZ,
        ambiguous="NaT",
        nonexistent="shift_forward"
    )

    # Drop rows where we could not resolve Israel local time (very rare DST edge cases)
    mask_valid = open_server.notna()
    open_server = open_server[mask_valid]
    close_server = close_server[mask_valid]
    df_full = df_full.loc[mask_valid].reset_index(drop=True)


    # Two views: Server (Israel) + UK (GMT/BST-aware)
    df_full["OpenTime_Server"] = open_server
    df_full["OpenTime_UK"] = df_full["OpenTime_Server"].dt.tz_convert(UK_TZ)

    df_full["CloseTime_Server"] = close_server
    df_full["CloseTime_UK"] = df_full["CloseTime_Server"].dt.tz_convert(UK_TZ)

    # --- Date range filter applied in Israel time ---
    # Convert your UTC-like START/END strings into Israel local datetimes
    start_dt_server = pd.to_datetime(START_DATE, utc=True).tz_convert(ISRAEL_TZ)
    end_dt_server   = pd.to_datetime(END_DATE,   utc=True).tz_convert(ISRAEL_TZ)

    print(f"[DEBUG] Raw rows from API: {len(df_full)}")
    print(f"[DEBUG] OpenTime_Server min: {df_full['OpenTime_Server'].min()} | "
          f"max: {df_full['OpenTime_Server'].max()}")

    # Filter on Israel server time
    mask = (df_full["OpenTime_Server"] >= start_dt_server) & (df_full["OpenTime_Server"] <= end_dt_server)
    df_full = df_full.loc[mask].reset_index(drop=True)

    print(f"[DEBUG] Rows after OpenTime_Server filter: {len(df_full)}")
    if "UserID" in df_full.columns:
        print(f"[DEBUG] Unique UserID count: {df_full['UserID'].nunique()}")

    # --- Drop tz for Excel compatibility (All Data sheet) ---
    for col in ["OpenTime_Server", "OpenTime_UK",
                "CloseTime_Server", "CloseTime_UK"]:
        if col in df_full.columns and isinstance(df_full[col].dtype, DatetimeTZDtype):
            df_full[col] = df_full[col].dt.tz_localize(None)

    # --- Sheet 2: Filtered subset ---
    filtered_columns = {
        "UserID": "User ID",
        "InstrumentName": "Instrument",
        "Amount": "Amount",
        "AmountLots": "Lots",
        "ActionType": "Action",  # We'll convert values below
        # Times (two views for analysis)
        "OpenTime_Server": "Open Time (Server - Israel Local)",
        "OpenTime_UK": "Open Time (UK - GMT/BST)",
        "OpenRate": "Open Rate",
        "CloseTime_Server": "Close Time (Server - Israel Local)",
        "CloseTime_UK": "Close Time (UK - GMT/BST)",
        "CloseRate": "Close Rate",
        "ProfitInAccountCurrency": "Profit (Account Currency)"
    }

    # Guard in case some columns are missing for any reason
    available_cols = [c for c in filtered_columns.keys() if c in df_full.columns]
    df_filtered = df_full[available_cols].rename(
        columns={k: v for k, v in filtered_columns.items() if k in available_cols}
    )

    # --- Convert ActionType numeric values to text (BUY/SELL) ---
    if "Action" in df_filtered.columns:
        df_filtered["Action"] = df_filtered["Action"].map({0: "BUY", 1: "SELL"}).fillna("UNKNOWN")

    # --- Ensure time columns are clean, readable strings ---
    time_cols = [
        "Open Time (Server - Israel Local)",
        "Open Time (UK - GMT/BST)",
        "Close Time (Server - Israel Local)",
        "Close Time (UK - GMT/BST)",
    ]
    for col in time_cols:
        if col in df_filtered.columns:
            df_filtered[col] = pd.to_datetime(df_filtered[col], errors="coerce").dt.strftime(
                "%Y-%m-%d %H:%M:%S"
            )

    # --- Save both sheets ---
    print(f"[-] Writing data to Excel file at:\n   {OUTPUT_FILE}")

    with pd.ExcelWriter(OUTPUT_FILE, engine="xlsxwriter") as writer:
        df_full.to_excel(writer, sheet_name="All Data", index=False)
        df_filtered.to_excel(writer, sheet_name="Filtered Data", index=False)

        workbook = writer.book
        worksheet = writer.sheets["Filtered Data"]

        # Formats
        format_buy = workbook.add_format({'bg_color': '#C6EFCE', 'font_color': '#006100'})
        format_sell = workbook.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006'})
        format_profit_positive = workbook.add_format({'font_color': '#0000FF'})
        format_profit_negative = workbook.add_format({'font_color': '#9C0006'})

        # Column indices
        if "Action" in df_filtered.columns:
            action_col_idx = df_filtered.columns.get_loc("Action")
        else:
            action_col_idx = None

        if "Profit (Account Currency)" in df_filtered.columns:
            profit_col_idx = df_filtered.columns.get_loc("Profit (Account Currency)")
        else:
            profit_col_idx = None

        # Conditional formatting for Action
        if action_col_idx is not None:
            worksheet.conditional_format(
                1, action_col_idx, len(df_filtered), action_col_idx,
                {'type': 'text', 'criteria': 'containing', 'value': 'BUY', 'format': format_buy}
            )
            worksheet.conditional_format(
                1, action_col_idx, len(df_filtered), action_col_idx,
                {'type': 'text', 'criteria': 'containing', 'value': 'SELL', 'format': format_sell}
            )

        # Conditional formatting for Profit
        if profit_col_idx is not None:
            worksheet.conditional_format(
                1, profit_col_idx, len(df_filtered), profit_col_idx,
                {'type': 'cell', 'criteria': '>', 'value': 0, 'format': format_profit_positive}
            )
            worksheet.conditional_format(
                1, profit_col_idx, len(df_filtered), profit_col_idx,
                {'type': 'cell', 'criteria': '<', 'value': 0, 'format': format_profit_negative}
            )

        # Auto column width
        for i, col in enumerate(df_filtered.columns):
            max_len = max(df_filtered[col].astype(str).map(len).max(), len(col)) + 2
            worksheet.set_column(i, i, max_len)

        # Freeze header row
        worksheet.freeze_panes(1, 0)

    print(f"[INFO] Excel report saved successfully!\n"
          f"[INFO] File path: {OUTPUT_FILE}")
    print(f"[INFO] Total closed positions exported: {len(df_full)}")


def main():
    print("[DONE] Starting closed positions export process...")
    print(f"[-] Date range (UTC strings): {START_DATE} → {END_DATE}")
    print(f"[-] Groups: {GROUPS}\n")

    start_time = time.time()

    # Step 1: Fetch data
    closed_positions = fetch_closed_positions_for_groups(GROUPS, START_DATE, END_DATE)

    # Step 2: Save to Excel
    create_excel_report(closed_positions)

    end_time = time.time()
    print(f"\n[DONE] Completed in {end_time - start_time:.2f} seconds.")


if __name__ == "__main__":
    main()
