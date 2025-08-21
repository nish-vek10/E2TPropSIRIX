# We'll run the "after first innings" simulator with the user's season-to-date data
# and show two demonstration scenarios. You can copy/paste this whole block into
# your own Python file or a notebook and tweak the final calls.

def rw_diff(total_rf, total_wl, total_ra, total_wt):
    a = total_rf / total_wl if total_wl else 0.0
    b = total_ra / total_wt if total_wt else 0.0
    return a - b

def totals(rf, wl, ra, wt):
    return sum(rf), sum(wl), sum(ra), sum(wt)

# ======== Season to date (from earlier messages) ========
htcc_runs_for = [197, 332, 228, 129, 217, 391, 136]
htcc_wickets_lost = [7, 7, 9, 10, 9, 6, 4]
htcc_runs_against = [193, 124, 169, 132, 60, 107, 135]
htcc_wickets_taken = [10, 10, 10, 4, 10, 10, 10]

ching_runs_for = [241, 220, 146, 132, 287, 322, 186]
ching_wickets_lost = [2, 3, 1, 4, 9, 7, 8]
ching_runs_against = [235, 219, 145, 129, 188, 323, 185]
ching_wickets_taken = [7, 8, 9, 10, 10, 7, 10]

HT_RF0, HT_WL0, HT_RA0, HT_WT0 = totals(htcc_runs_for, htcc_wickets_lost, htcc_runs_against, htcc_wickets_taken)
CH_RF0, CH_WL0, CH_RA0, CH_WT0 = totals(ching_runs_for, ching_wickets_lost, ching_runs_against, ching_wickets_taken)

def after_first_innings_head_to_head(
    # HTCC knowns (use None for unknowns)
    ht_last_rf=None, ht_last_wl=None, ht_last_ra=None, ht_last_wt=None,
    # Chingford knowns (use None for unknowns)
    ch_last_rf=None, ch_last_wl=None, ch_last_ra=None, ch_last_wt=None,
    # Search ranges for the UNKNOWN side of each game (second innings still to come)
    # HTCC unknowns if they bowled first: we search RF, WL
    ht_rf_range=range(120, 401, 10), ht_wl_range=range(2, 11),
    # HTCC unknowns if they batted first: we search RA, WT
    ht_ra_range=range(120, 401, 10), ht_wt_range=range(2, 11),
    # Chingford unknowns if they bowled first: search RF, WL
    ch_rf_range=range(120, 401, 10), ch_wl_range=range(2, 11),
    # Chingford unknowns if they batted first: search RA, WT
    ch_ra_range=range(120, 401, 10), ch_wt_range=range(2, 11),
    # Output
    top_n=10
):
    # Build all possibilities for each team’s last-match totals based on what is still unknown
    ht_poss = []
    if ht_last_rf is not None and ht_last_wl is not None:
        # HTCC batted first; search bowling figures to come
        for ra in ht_ra_range:
            for wt in ht_wt_range:
                ht_poss.append((ht_last_rf, ht_last_wl, ra, wt))
    elif ht_last_ra is not None and ht_last_wt is not None:
        # HTCC bowled first; search batting chase to come
        for rf in ht_rf_range:
            for wl in ht_wl_range:
                ht_poss.append((rf, wl, ht_last_ra, ht_last_wt))
    else:
        print("HTCC: supply either (rf, wl) OR (ra, wt) from the first innings.")
        return []

    ch_poss = []
    if ch_last_rf is not None and ch_last_wl is not None:
        # Chingford batted first; search bowling figures to come
        for ra in ch_ra_range:
            for wt in ch_wt_range:
                ch_poss.append((ch_last_rf, ch_last_wl, ra, wt))
    elif ch_last_ra is not None and ch_last_wt is not None:
        # Chingford bowled first; search batting chase to come
        for rf in ch_rf_range:
            for wl in ch_wl_range:
                ch_poss.append((rf, wl, ch_last_ra, ch_last_wt))
    else:
        print("Chingford: supply either (rf, wl) OR (ra, wt) from the first innings.")
        return []

    # Search all combinations; keep only those where HTCC finishes ahead
    results = []
    for (ht_rf, ht_wl, ht_ra, ht_wt) in ht_poss:
        ht_final_diff = rw_diff(HT_RF0 + ht_rf, HT_WL0 + ht_wl, HT_RA0 + ht_ra, HT_WT0 + ht_wt)

        for (ch_rf, ch_wl, ch_ra, ch_wt) in ch_poss:
            ch_final_diff = rw_diff(CH_RF0 + ch_rf, CH_WL0 + ch_wl, CH_RA0 + ch_ra, CH_WT0 + ch_wt)

            if ht_final_diff > ch_final_diff:
                # Rank by "easiest HTCC path": fewer WL, fewer RA, lower RF needed, more WT
                results.append((
                    ht_wl, ht_ra, ht_rf, -ht_wt,   # sort keys (easiest first)
                    ht_final_diff, ch_final_diff,
                    (ht_rf, ht_wl, ht_ra, ht_wt),
                    (ch_rf, ch_wl, ch_ra, ch_wt),
                ))

    if not results:
        print("No overtake scenarios found in the searched ranges.")
        return []

    results.sort()
    picks = results[:top_n]

    print("=== After First Innings What-If (HTCC must finish ahead) ===")
    for i, row in enumerate(picks, 1):
        _, _, _, _, ht_diff, ch_diff, (ht_rf, ht_wl, ht_ra, ht_wt), (ch_rf, ch_wl, ch_ra, ch_wt) = row
        print(
            f"{i:>2}. HTCC second-innings outcome -> RF={ht_rf}, WL={ht_wl}, RA={ht_ra}, WT={ht_wt} "
            f"=> HT diff {ht_diff:+.3f} | Chingford outcome -> RF={ch_rf}, WL={ch_wl}, RA={ch_ra}, WT={ch_wt} "
            f"=> Ch diff {ch_diff:+.3f}"
        )

    return [{
        "htcc_rf": ht_rf, "htcc_wl": ht_wl, "htcc_ra": ht_ra, "htcc_wt": ht_wt,
        "ching_rf": ch_rf, "ching_wl": ch_wl, "ching_ra": ch_ra, "ching_wt": ch_wt,
        "htcc_final_diff": float(ht_diff), "ching_final_diff": float(ch_diff)
    } for _, _, _, _, ht_diff, ch_diff, (ht_rf, ht_wl, ht_ra, ht_wt), (ch_rf, ch_wl, ch_ra, ch_wt) in picks]


print("=== Demo A: HTCC batted first 240/7; Chingford batted first 260/6 ===")
_ = after_first_innings_head_to_head(
    ht_last_rf=240, ht_last_wl=7, ht_last_ra=None, ht_last_wt=None,
    ch_last_rf=260, ch_last_wl=6, ch_last_ra=None, ch_last_wt=None,
    ht_ra_range=range(150, 351, 10), ht_wt_range=range(2, 11),
    ch_ra_range=range(150, 351, 10), ch_wt_range=range(2, 11),
    top_n=6
)

print("\n=== Demo B: HTCC bowled first, opp 205/9; Chingford bowled first, opp 220/8 ===")
_ = after_first_innings_head_to_head(
    ht_last_rf=None, ht_last_wl=None, ht_last_ra=205, ht_last_wt=9,
    ch_last_rf=None, ch_last_wl=None, ch_last_ra=220, ch_last_wt=8,
    ht_rf_range=range(150, 331, 10), ht_wl_range=range(2, 11),
    ch_rf_range=range(150, 331, 10), ch_wl_range=range(2, 11),
    top_n=6
)
