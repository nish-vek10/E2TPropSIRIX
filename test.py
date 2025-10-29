#
# def rw_summary(team, runs_for, wkts_lost, runs_against, wkts_taken):
#     # Basic validation
#     n = {len(runs_for), len(wkts_lost), len(runs_against), len(wkts_taken)}
#     if len(n) != 1:
#         raise ValueError(f"{team}: input lists must be the same length.")
#     if not runs_for:
#         raise ValueError(f"{team}: no matches provided.")
#
#     # Totals
#     total_rf = sum(runs_for)
#     total_wl = sum(wkts_lost)
#     total_ra = sum(runs_against)
#     total_wt = sum(wkts_taken)
#
#     # Averages (guard against zero wickets)
#     avg_rpw_for = total_rf / total_wl if total_wl else 0.0
#     avg_rpw_against = total_ra / total_wt if total_wt else 0.0
#
#     # Differential
#     diff = avg_rpw_for - avg_rpw_against
#
#     # Print summary
#     print(f"=== {team} R/W Summary ===")
#     print(f"Matches: {len(runs_for)}")
#     print(f"Total Runs For: {total_rf}")
#     print(f"Total Wickets Lost: {total_wl}")
#     print(f"Average R/W For: {avg_rpw_for:.2f}")
#     print(f"Total Runs Against: {total_ra}")
#     print(f"Total Wickets Taken: {total_wt}")
#     print(f"Average R/W Against: {avg_rpw_against:.2f}")
#     print(f"R/W Differential: {diff:+.2f}")
#     print()
#     return {
#         "team": team,
#         "matches": len(runs_for),
#         "total_runs_for": total_rf,
#         "total_wkts_lost": total_wl,
#         "avg_rpw_for": avg_rpw_for,
#         "total_runs_against": total_ra,
#         "total_wkts_taken": total_wt,
#         "avg_rpw_against": avg_rpw_against,
#         "rw_diff": diff,
#     }
#
# # ================== DATA ==================
# # HTCC
# htcc_runs_for = [197, 332, 228, 129, 217, 391, 136]
# htcc_wickets_lost = [7, 7, 9, 10, 9, 6, 4]
# htcc_runs_against = [193, 124, 169, 132, 60, 107, 135]
# htcc_wickets_taken = [10, 10, 10, 4, 10, 10, 10]
#
# # Chingford
# ching_runs_for = [241, 220, 146, 132, 287, 322, 186]
# ching_wickets_lost = [2, 3, 1, 4, 9, 7, 8]
# ching_runs_against = [235, 219, 145, 129, 188, 323, 185]
# ching_wickets_taken = [7, 8, 9, 10, 10, 7, 10]
#
# # ================== RUN ==================
# htcc = rw_summary("HTCC", htcc_runs_for, htcc_wickets_lost, htcc_runs_against, htcc_wickets_taken)
# ching = rw_summary("Chingford", ching_runs_for, ching_wickets_lost, ching_runs_against, ching_wickets_taken)
#
# # Optional: quick comparison
# print("=== R/W Differential Comparison ===")
# if htcc["rw_diff"] > ching["rw_diff"]:
#     print(f"HTCC ahead by {htcc['rw_diff'] - ching['rw_diff']:+.2f}")
# elif ching["rw_diff"] > htcc["rw_diff"]:
#     print(f"Chingford ahead by {ching['rw_diff'] - htcc['rw_diff']:+.2f}")
# else:
#     print("Level on R/W differential.")
#
# def final_diff(runs_for, wkts_lost, runs_against, wkts_taken):
#     avg_for = runs_for / wkts_lost if wkts_lost else 0
#     avg_against = runs_against / wkts_taken if wkts_taken else 0
#     return avg_for - avg_against





import requests
import pandas as pd
from datetime import datetime, timedelta, timezone

# === OANDA CONFIG (simple requests) — UNCHANGED === #
oanda_token       = "37ee33b35f88e073a08d533849f7a24b-524c89ef15f36cfe532f0918a6aee4c2"
oanda_api_base    = "https://api-fxpractice.oanda.com/v3"

HEADERS = {"Authorization": f"Bearer {oanda_token}"}


def iso_utc(dt: datetime) -> str:
    """Return RFC3339/ISO string with trailing 'Z' for UTC-aware datetimes."""
    if dt.tzinfo is None:
        raise ValueError("iso_utc() requires a timezone-aware datetime")
    return dt.isoformat().replace("+00:00", "Z")


def get_ohlc(instrument: str, granularity: str, start: datetime, end: datetime) -> pd.DataFrame:
    """
    Fetch OHLC candles (mid) for an instrument between start and end at a given granularity.
    Returns DataFrame: time (UTC), open, high, low, close
    """
    url = f"{oanda_api_base}/instruments/{instrument}/candles"
    params = {
        "granularity": granularity,
        "price": "M",
        "from": iso_utc(start),
        "to": iso_utc(end),
    }

    resp = requests.get(url, headers=HEADERS, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    records = []
    for c in data.get("candles", []):
        if c.get("complete"):
            records.append({
                "time": c["time"],
                "open": float(c["mid"]["o"]),
                "high": float(c["mid"]["h"]),
                "low":  float(c["mid"]["l"]),
                "close": float(c["mid"]["c"]),
            })

    df = pd.DataFrame.from_records(records)
    if df.empty:
        return df

    df["time"] = pd.to_datetime(df["time"], utc=True)
    df = df.sort_values("time").reset_index(drop=True)
    return df


def print_last5(label_asset: str, df: pd.DataFrame) -> None:
    """Print the latest 5 rows (time, close) for an asset."""
    if df.empty:
        print(f"{label_asset}: (no data)")
        return
    tail5 = df[["time", "close"]].tail(5)
    print(f"{label_asset} — last 5:")
    print(tail5.to_string(index=False))


def corr_for_period_and_tf(months: int, granularity: str, tf_label: str) -> None:
    """Compute ETH vs XAU correlation for the window and timeframe; print latest 5 for both assets."""
    end = datetime.now(timezone.utc)
    # Approximate months as 30 days each; swap for calendar math if desired.
    start = end - timedelta(days=30 * months)

    print(f"\n=== Period: {months} months | Timeframe: {tf_label} ({granularity}) ===")
    print(f"Window (UTC): {start:%Y-%m-%d %H:%M}  →  {end:%Y-%m-%d %H:%M}")

    # Fetch
    eth = get_ohlc("ETH_USD", granularity, start, end)
    xau = get_ohlc("XAU_USD", granularity, start, end)

    print(f"Fetched rows — ETH: {len(eth)}, XAU: {len(xau)}")

    # Show only last 5 samples for each asset (latest)
    print_last5("ETH_USD", eth)
    print_last5("XAU_USD", xau)

    if eth.empty or xau.empty:
        print("Data missing; skipping correlation.")
        return

    # Align on timestamp and compute correlation on closes
    merged = pd.merge(
        eth[["time", "close"]].rename(columns={"close": "close_eth"}),
        xau[["time", "close"]].rename(columns={"close": "close_xau"}),
        on="time",
        how="inner"
    ).dropna()

    print(f"Aligned rows: {len(merged)}")
    if merged.empty:
        print("No overlapping timestamps; skipping correlation.")
        return

    corr = merged["close_eth"].corr(merged["close_xau"])
    print(f"CORRELATION COEFFICIENT: (ETH_USD vs XAU_USD): {corr:.6f}")


def main():
    # Timeframes: 1H, 4H, 1D
    timeframes = [
        ("H1", "1H"),
        ("H4", "4H"),
        ("D",  "1D"),
    ]

    # Periods: 3 months and 6 months
    for months in (3, 6):
        for granularity, tf_label in timeframes:
            corr_for_period_and_tf(months, granularity, tf_label)


if __name__ == "__main__":
    main()
