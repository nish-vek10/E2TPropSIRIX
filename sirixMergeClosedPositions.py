import os
import glob
import time
import pandas as pd
from datetime import datetime, timezone

import warnings
warnings.filterwarnings(
    "ignore",
    message="The behavior of DataFrame concatenation with empty or all-NA entries is deprecated"
)


# =========================
# CONFIG
# =========================
INPUT_DIR = r"C:\Users\anish\OneDrive\Desktop\Anish\SIRIX DATA\ClosedPositions_AllUsers_PerUser"
OUTPUT_DIR = INPUT_DIR
RUN_TAG = "v1"

BLANK_ROWS_BETWEEN = 1  # "one empty row" between users

UTC_NOW = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
OUT_FILE = os.path.join(OUTPUT_DIR, f"00_merged_closed_trades_ALL_USERS_{RUN_TAG}.xlsx")

SHEET_NAME = "Trades"

# columns used in your per-user exports
TIME_COLS = ["OpenTime_server", "CloseTime_server"]


def fmt_mm_ss(seconds: float) -> str:
    m = int(seconds // 60)
    s = int(seconds % 60)
    return f"{m:02d}:{s:02d}"


def main():
    start_ts = time.time()

    files = sorted(glob.glob(os.path.join(INPUT_DIR, "*_closedTrades.xlsx")))
    print(f"[INFO] Files found: {len(files)}")
    if not files:
        print("[DONE] No files to merge.")
        return

    merged_blocks = []
    columns_ref = None

    for i, fp in enumerate(files, start=1):
        print(f"[{i}/{len(files)}] Reading {os.path.basename(fp)}")
        df = pd.read_excel(fp, sheet_name=SHEET_NAME)

        if df.empty:
            continue

        # lock the columns layout to the first non-empty file
        if columns_ref is None:
            columns_ref = df.columns.tolist()
        else:
            # enforce same column order (add missing cols if any)
            for c in columns_ref:
                if c not in df.columns:
                    df[c] = pd.NA
            df = df[columns_ref]

        merged_blocks.append(df)

        # separator row(s) — NOT all-NA (avoids FutureWarning)
        if BLANK_ROWS_BETWEEN > 0:
            blank_row = {c: pd.NA for c in columns_ref}
            blank_row[columns_ref[0]] = ""  # harmless non-NA so pandas doesn't warn
            blank_df = pd.DataFrame([blank_row] * BLANK_ROWS_BETWEEN)
            merged_blocks.append(blank_df)

    # remove trailing blank block at the end (optional)
    if merged_blocks and isinstance(merged_blocks[-1], pd.DataFrame):
        last = merged_blocks[-1]
        if last.shape[0] == BLANK_ROWS_BETWEEN and last.notna().sum().sum() == 0:
            merged_blocks = merged_blocks[:-1]

    final_df = pd.concat(merged_blocks, ignore_index=True)

    print("[INFO] Writing merged file:")
    print(f"  {OUT_FILE}")

    # write + APPLY FORMATTING AGAIN
    with pd.ExcelWriter(OUT_FILE, engine="xlsxwriter", datetime_format="yyyy-mm-dd hh:mm:ss") as writer:
        final_df.to_excel(writer, sheet_name=SHEET_NAME, index=False)

        wb = writer.book
        ws = writer.sheets[SHEET_NAME]

        # Header formatting (light gray)
        header_fmt = wb.add_format({"bold": True, "bg_color": "#D9D9D9", "border": 1})
        ws.set_row(0, None, header_fmt)
        ws.freeze_panes(1, 0)

        # datetime column format
        dt_fmt = wb.add_format({"num_format": "yyyy-mm-dd hh:mm:ss"})

        # conditional formats
        fmt_buy  = wb.add_format({"bg_color": "#C6EFCE", "font_color": "#006100"})
        fmt_sell = wb.add_format({"bg_color": "#FFC7CE", "font_color": "#9C0006"})
        fmt_pos  = wb.add_format({"font_color": "#0000FF"})  # blue
        fmt_neg  = wb.add_format({"font_color": "#9C0006"})  # red
        fmt_bold = wb.add_format({"bold": True})

        # auto column width (capped)
        for col_idx, col in enumerate(final_df.columns):
            ser = final_df[col].astype(str)
            max_len = max(ser.map(len).max() if len(ser) else 0, len(col)) + 2
            ws.set_column(col_idx, col_idx, min(max_len, 60))

        # apply datetime width + format
        for c in TIME_COLS:
            if c in final_df.columns:
                idx = final_df.columns.get_loc(c)
                ws.set_column(idx, idx, 20, dt_fmt)

        # Action BUY/SELL cell background
        if "Action" in final_df.columns:
            c = final_df.columns.get_loc("Action")
            ws.conditional_format(1, c, len(final_df), c, {
                "type": "text", "criteria": "containing", "value": "BUY", "format": fmt_buy
            })
            ws.conditional_format(1, c, len(final_df), c, {
                "type": "text", "criteria": "containing", "value": "SELL", "format": fmt_sell
            })

        # ClosedProfit red/blue font
        if "ClosedProfit" in final_df.columns:
            c = final_df.columns.get_loc("ClosedProfit")
            ws.conditional_format(1, c, len(final_df), c, {
                "type": "cell", "criteria": ">", "value": 0, "format": fmt_pos
            })
            ws.conditional_format(1, c, len(final_df), c, {
                "type": "cell", "criteria": "<", "value": 0, "format": fmt_neg
            })

        # Bold Zero-Balance InstrumentName
        if "InstrumentName" in final_df.columns:
            c = final_df.columns.get_loc("InstrumentName")
            ws.conditional_format(1, c, len(final_df), c, {
                "type": "text", "criteria": "containing", "value": "Zero-Balance", "format": fmt_bold
            })

    elapsed = time.time() - start_ts
    print(f"[DONE] Merge completed successfully in {fmt_mm_ss(elapsed)} (MM:SS)")


if __name__ == "__main__":
    main()
