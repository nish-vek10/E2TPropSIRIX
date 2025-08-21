
def rw_summary(team, runs_for, wkts_lost, runs_against, wkts_taken):
    # Basic validation
    n = {len(runs_for), len(wkts_lost), len(runs_against), len(wkts_taken)}
    if len(n) != 1:
        raise ValueError(f"{team}: input lists must be the same length.")
    if not runs_for:
        raise ValueError(f"{team}: no matches provided.")

    # Totals
    total_rf = sum(runs_for)
    total_wl = sum(wkts_lost)
    total_ra = sum(runs_against)
    total_wt = sum(wkts_taken)

    # Averages (guard against zero wickets)
    avg_rpw_for = total_rf / total_wl if total_wl else 0.0
    avg_rpw_against = total_ra / total_wt if total_wt else 0.0

    # Differential
    diff = avg_rpw_for - avg_rpw_against

    # Print summary
    print(f"=== {team} R/W Summary ===")
    print(f"Matches: {len(runs_for)}")
    print(f"Total Runs For: {total_rf}")
    print(f"Total Wickets Lost: {total_wl}")
    print(f"Average R/W For: {avg_rpw_for:.2f}")
    print(f"Total Runs Against: {total_ra}")
    print(f"Total Wickets Taken: {total_wt}")
    print(f"Average R/W Against: {avg_rpw_against:.2f}")
    print(f"R/W Differential: {diff:+.2f}")
    print()
    return {
        "team": team,
        "matches": len(runs_for),
        "total_runs_for": total_rf,
        "total_wkts_lost": total_wl,
        "avg_rpw_for": avg_rpw_for,
        "total_runs_against": total_ra,
        "total_wkts_taken": total_wt,
        "avg_rpw_against": avg_rpw_against,
        "rw_diff": diff,
    }

# ================== DATA ==================
# HTCC
htcc_runs_for = [197, 332, 228, 129, 217, 391, 136]
htcc_wickets_lost = [7, 7, 9, 10, 9, 6, 4]
htcc_runs_against = [193, 124, 169, 132, 60, 107, 135]
htcc_wickets_taken = [10, 10, 10, 4, 10, 10, 10]

# Chingford
ching_runs_for = [241, 220, 146, 132, 287, 322, 186]
ching_wickets_lost = [2, 3, 1, 4, 9, 7, 8]
ching_runs_against = [235, 219, 145, 129, 188, 323, 185]
ching_wickets_taken = [7, 8, 9, 10, 10, 7, 10]

# ================== RUN ==================
htcc = rw_summary("HTCC", htcc_runs_for, htcc_wickets_lost, htcc_runs_against, htcc_wickets_taken)
ching = rw_summary("Chingford", ching_runs_for, ching_wickets_lost, ching_runs_against, ching_wickets_taken)

# Optional: quick comparison
print("=== R/W Differential Comparison ===")
if htcc["rw_diff"] > ching["rw_diff"]:
    print(f"HTCC ahead by {htcc['rw_diff'] - ching['rw_diff']:+.2f}")
elif ching["rw_diff"] > htcc["rw_diff"]:
    print(f"Chingford ahead by {ching['rw_diff'] - htcc['rw_diff']:+.2f}")
else:
    print("Level on R/W differential.")

def final_diff(runs_for, wkts_lost, runs_against, wkts_taken):
    avg_for = runs_for / wkts_lost if wkts_lost else 0
    avg_against = runs_against / wkts_taken if wkts_taken else 0
    return avg_for - avg_against
