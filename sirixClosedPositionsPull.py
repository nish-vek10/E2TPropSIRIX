import os
import requests
import pandas as pd
from datetime import datetime, timezone
import time

# ---------------- CONFIGURATION ----------------
BASE_URL = "https://restapi-real3.sirixtrader.com"
TOKEN = "t1_a7xeQOJPnfBzuCncH60yjLFu"
TRADER_ID = "188320"

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json"
}

# --- Directory to save report ---
SAVE_DIR = r"C:\Users\anish\OneDrive\Desktop\Anish\SIRIX DATA"
os.makedirs(SAVE_DIR, exist_ok=True)

# ---  File name with timestamp ---
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
OUTPUT_FILE = os.path.join(SAVE_DIR, f"closed_positions_report_{timestamp}.xlsx")

# Date range: From June 1st, 2025 00:00:00 until now
START_DATE = "2025-06-01T00:00:00Z"
END_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

# ------------------------------------------------


def fetch_closed_positions(start_time, end_time):
    """
    Fetch closed positions from the API for the given time range.
    """
    url = f"{BASE_URL}/api/ManagementService/GetClosedPoistions"

    payload = {
        "startTime": start_time,
        "endTime": end_time
    }

    print("[-] Sending request to fetch closed positions...")
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

    # --- Sheet 2: Filtered subset ---
    filtered_columns = {
        "UserID": "User ID",
        "InstrumentName": "Instrument",
        "Amount": "Amount",
        "AmountLots": "Lots",
        "ActionType": "Action",  # We'll convert values below
        "OpenTime": "Open Time",
        "OpenRate": "Open Rate",
        "CloseTime": "Close Time",
        "CloseRate": "Close Rate",
        "ProfitInAccountCurrency": "Profit (Account Currency)"
    }

    df_filtered = df_full[list(filtered_columns.keys())].rename(columns=filtered_columns)

    # --- Convert ActionType numeric values to text (BUY/SELL) ---
    df_filtered["Action"] = df_filtered["Action"].map({0: "BUY", 1: "SELL"}).fillna("UNKNOWN")

    # --- Convert time strings to readable format ---
    for col in ["Open Time", "Close Time"]:
        df_filtered[col] = pd.to_datetime(df_filtered[col], errors='coerce').dt.strftime("%Y-%m-%d %H:%M:%S")

    # --- Save both sheets ---
    print(f"[-] Writing data to Excel file at:\n   {OUTPUT_FILE}")

    with pd.ExcelWriter(OUTPUT_FILE, engine="xlsxwriter") as writer:
        df_full.to_excel(writer, sheet_name="All Data", index=False)
        df_filtered.to_excel(writer, sheet_name="Filtered Data", index=False)

        # Get workbook and worksheet for formatting
        workbook = writer.book
        worksheet = writer.sheets["Filtered Data"]

        # === ADD COLOR FORMATS ===
        format_buy = workbook.add_format({'bg_color': '#C6EFCE', 'font_color': '#006100'})   # light green
        format_sell = workbook.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006'})  # light red
        format_profit_positive = workbook.add_format({'font_color': '#0000FF'})              # blue
        format_profit_negative = workbook.add_format({'font_color': '#9C0006'})              # red

        # Find column indexes dynamically
        action_col_idx = df_filtered.columns.get_loc("Action")
        profit_col_idx = df_filtered.columns.get_loc("Profit (Account Currency)")

        # Apply conditional formatting for BUY/SELL column
        worksheet.conditional_format(
            1, action_col_idx, len(df_filtered), action_col_idx,
            {'type': 'text', 'criteria': 'containing', 'value': 'BUY', 'format': format_buy}
        )
        worksheet.conditional_format(
            1, action_col_idx, len(df_filtered), action_col_idx,
            {'type': 'text', 'criteria': 'containing', 'value': 'SELL', 'format': format_sell}
        )

        # Apply conditional formatting for Profit column
        worksheet.conditional_format(
            1, profit_col_idx, len(df_filtered), profit_col_idx,
            {'type': 'cell', 'criteria': '>', 'value': 0, 'format': format_profit_positive}
        )
        worksheet.conditional_format(
            1, profit_col_idx, len(df_filtered), profit_col_idx,
            {'type': 'cell', 'criteria': '<', 'value': 0, 'format': format_profit_negative}
        )

        # === OPTIONAL QUALITY OF LIFE FORMATTING ===
        # Auto-adjust column widths
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
    print(f"[-] Date range: {START_DATE} → {END_DATE}\n")

    start_time = time.time()

    # Step 1: Fetch data
    closed_positions = fetch_closed_positions(START_DATE, END_DATE)

    # Step 2: Save to Excel
    create_excel_report(closed_positions)

    end_time = time.time()
    print(f"\n[DONE] Completed in {end_time - start_time:.2f} seconds.")


if __name__ == "__main__":
    main()
