"""
XAUUSD M1 Raw Candles Export (OANDA → Excel)
- Uses midpoint (M) prices: (bid+ask)/2
- Columns: Open | High | Low | Close | Volume | Timestamp (UTC)
- Progress printed in terminal
- Full summary written at top of the Excel sheet (no overlap)
"""

import os
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
import time
from dateutil import tz

UK_TZ = tz.gettz("Europe/London")


# ========= USER SETTINGS =========
OANDA_TOKEN = "37ee33b35f88e073a08d533849f7a24b-524c89ef15f36cfe532f0918a6aee4c2"
INSTRUMENT  = "XAU_USD"
GRANULARITY = "M1"
OUTPUT_DIR = r"C:\Users\anish\OneDrive\Desktop\Anish\OANDA DATA\Latest-Data-DEC"

# Date strings (DD/MM/YYYY HH:MM:SS TZ)
START_STR = "04/12/2025 00:00:00"
# END_STR   = "11/12/2025 23:59:00"
END_STR = datetime.now(UK_TZ).strftime("%d/%m/%Y %H:%M:%S")

# Pull in chunks (OANDA returns up to ~5000)
BATCH_CANDLES = 5000
API_BASE = "https://api-fxpractice.oanda.com/v3"
HEADERS = {"Authorization": f"Bearer {OANDA_TOKEN}"}
# =================================

def parse_uk_local(s: str) -> datetime:
    """
    Parse a DD/MM/YYYY HH:MM:SS string as London local time
    (handles GMT/BST automatically) and return a UTC-aware datetime.
    """
    # Parse as naive (no timezone yet) – pandas will ignore the 'GMT' text safely
    dt_naive = pd.to_datetime(s, dayfirst=True).to_pydatetime()
    # Attach London tz (with DST rules) and convert to UTC
    dt_local = dt_naive.replace(tzinfo=UK_TZ)
    return dt_local.astimezone(timezone.utc)

def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

def fetch_candles(instrument: str, start_dt: datetime, end_dt: datetime):
    cur = start_dt
    total_minutes = int((end_dt - start_dt).total_seconds() // 60)
    pulled = 0
    batch_idx = 0

    while cur <= end_dt:
        batch_idx += 1
        batch_end = min(cur + timedelta(minutes=BATCH_CANDLES - 1), end_dt)

        params = {
            "granularity": GRANULARITY,
            "from": iso(cur),
            "to": iso(batch_end),
            "price": "M",                # midpoint OHLC
            "includeFirst": "true"
        }

        t0 = time.time()
        r = requests.get(f"{API_BASE}/instruments/{instrument}/candles", headers=HEADERS, params=params)
        if r.status_code != 200:
            time.sleep(1.5)
            r = requests.get(f"{API_BASE}/instruments/{instrument}/candles", headers=HEADERS, params=params)
            if r.status_code != 200:
                raise RuntimeError(f"OANDA error {r.status_code}: {r.text}")
        dt_ms = (time.time() - t0) * 1000

        raw = r.json().get("candles", [])
        completed = [c for c in raw if c.get("complete", False)]
        pulled += len(completed)
        pct = (min(pulled, total_minutes + 1) / (total_minutes + 1)) * 100 if total_minutes > 0 else 100.0

        print(f"[BATCH {batch_idx:02d}] {cur.strftime('%Y-%m-%d %H:%M')} → {batch_end.strftime('%Y-%m-%d %H:%M')} | "
              f"got {len(completed):4d} | total {pulled:6d}/{total_minutes+1} ({pct:5.1f}%) | {dt_ms:.0f} ms")

        yield completed

        if not completed:
            cur = batch_end + timedelta(minutes=1)
        else:
            last_ts = pd.to_datetime(completed[-1]["time"], utc=True).to_pydatetime()
            cur = last_ts + timedelta(minutes=1)

def build_dataframe(candles) -> pd.DataFrame:
    rows = []
    for c in candles:
        # OANDA time is UTC
        t_utc = pd.to_datetime(c["time"], utc=True)
        # Convert to UK local (handles GMT/BST automatically)
        t_uk = t_utc.tz_convert(UK_TZ)

        mid = c["mid"]
        rows.append({
            "Open":  float(mid["o"]),
            "High":  float(mid["h"]),
            "Low":   float(mid["l"]),
            "Close": float(mid["c"]),
            "Volume": int(c.get("volume", 0)),

            # Candle open in UTC
            "Timestamp_UTC": t_utc.strftime("%Y-%m-%d %H:%M:%S"),

            # Candle open/close in UK local time (GMT/BST)
            "Open Time (UK - GMT/BST)":  t_uk.strftime("%Y-%m-%d %H:%M:%S"),
            "Close Time (UK - GMT/BST)": (t_uk + timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M:%S"),
        })

    return pd.DataFrame(
        rows,
        columns=[
            "Open", "High", "Low", "Close", "Volume",
            "Timestamp_UTC",
            "Open Time (UK - GMT/BST)",
            "Close Time (UK - GMT/BST)",
        ],
    )

def save_to_excel(df: pd.DataFrame, path: str, title: str, meta: dict):
    """
    Writes a summary block and then the table below it, using a dynamic start row
    so the two never overlap.
    """
    with pd.ExcelWriter(path, engine="xlsxwriter", datetime_format="yyyy-mm-dd hh:mm:ss") as writer:
        sheet = "M1_Candles"
        wb  = writer.book

        # ----- Title -----
        ws  = wb.add_worksheet(sheet)
        writer.sheets[sheet] = ws
        title_fmt = wb.add_format({"bold": True, "font_size": 14})
        ws.write(0, 0, title, title_fmt)

        # ----- Summary/meta block -----
        key_fmt = wb.add_format({"bold": True})
        r = 2  # meta starts on row 2
        for k, v in meta.items():
            ws.write(r, 0, f"{k}:", key_fmt)
            ws.write(r, 1, str(v))
            r += 1

        # Compute a safe start row for data: one blank row after meta
        startrow = r + 1

        # ----- Data table -----
        df.to_excel(writer, sheet_name=sheet, index=False, startrow=startrow)

        # Header formatting
        header_fmt = wb.add_format({"bold": True, "bg_color": "#D9E1F2", "border": 1})
        for ci, col in enumerate(df.columns):
            ws.write(startrow, ci, col, header_fmt)

        # Autofilter & freeze header row
        ws.autofilter(startrow, 0, startrow + len(df), len(df.columns) - 1)
        ws.freeze_panes(startrow + 1, 0)

        # Column widths
        for i, col in enumerate(df.columns):
            max_len = max([len(str(col))] + [len(str(x)) for x in df[col].head(500).astype(str)])
            ws.set_column(i, i, min(max(12, max_len + 2), 30))

        # Footer note
        footer_fmt = wb.add_format({"italic": True, "font_color": "#555555"})
        ws.write(startrow + len(df) + 2, 0, f"Total candles: {len(df)}", footer_fmt)

def main():
    start_dt = parse_uk_local(START_STR)  # UK local → UTC
    end_dt = parse_uk_local(END_STR)  # UK local → UTC

    if end_dt < start_dt:
        raise ValueError("END_DT is earlier than START_DT.")

    # --- Build auto filename: {ticker}-1M_Data_{startdate}-{enddate}.xlsx ---
    # Dates formatted as DDMMYY, e.g. 01/08/2025 → '010825'
    start_label = start_dt.strftime("%d%m%y")
    end_label   = end_dt.strftime("%d%m%y")

    filename = f"{INSTRUMENT}-1M_Data_{start_label}-{end_label}.xlsx"
    output_path = os.path.join(OUTPUT_DIR, filename)

    print(f"[START] Pulling {INSTRUMENT} {GRANULARITY} from {start_dt} to {end_dt} (UTC)")
    all_rows = []
    batches = 0

    for batch in fetch_candles(INSTRUMENT, start_dt, end_dt):
        batches += 1
        all_rows.extend(batch)

    if not all_rows:
        print("[WARN] No candles retrieved for the requested window.")
        return

    df = build_dataframe(all_rows)
    df.sort_values("Timestamp_UTC", inplace=True, ignore_index=True)

    # For meta, also compute UK-local window from the UTC datetimes
    start_uk = start_dt.astimezone(UK_TZ)
    end_uk = end_dt.astimezone(UK_TZ)

    meta = {
        "Instrument": INSTRUMENT,
        "Granularity": GRANULARITY,
        "Time Window (UTC)": f"{start_dt.strftime('%Y-%m-%d %H:%M:%S')} → {end_dt.strftime('%Y-%m-%d %H:%M:%S')}",
        "Time Window (UK local)": f"{start_uk.strftime('%Y-%m-%d %H:%M:%S')} → {end_uk.strftime('%Y-%m-%d %H:%M:%S')}",
        "Price Type": "Midpoint (price=M) = (bid+ask)/2",
        "Total Rows": len(df),
        "Total Batches": batches,
        "First Candle (UTC Open)": df.iloc[0]["Timestamp_UTC"],
        "Last Candle (UTC Open)": df.iloc[-1]["Timestamp_UTC"],
        "First Candle (UK Open)": df.iloc[0]["Open Time (UK - GMT/BST)"],
        "Last Candle (UK Open)": df.iloc[-1]["Open Time (UK - GMT/BST)"],
        "Data Source": "OANDA v3 API",
    }

    title = f"{INSTRUMENT} — {GRANULARITY} Raw Candles Export (Mid Prices)"
    save_to_excel(df, output_path, title, meta)

    # Terminal summary
    print("\n===== SUMMARY =====")
    for k, v in meta.items():
        print(f"{k:>22}: {v}")
    print(f"Saved to   : {output_path}")

if __name__ == "__main__":
    main()
