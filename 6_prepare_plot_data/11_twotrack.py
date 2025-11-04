# 11_twotrack.py
# ------------------------------------------------------------
# Step 11: Dual-constraint two-track partition
# Distinguishes overlapping biological years using both
# Adult_Total and Live_Shipped trends.
#
# Input : 100_Data/csv_plotdata.csv
# Output: 100_Data/11_twotrack_output.csv (+ updates csv_plotdata.csv)
# ------------------------------------------------------------

import pandas as pd
import numpy as np
from pathlib import Path

print("🏗️ Step 11: Dual-constraint two-track monotone partition...")

# ------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------
LONG_GAP_DAYS = 100            # reset protection
tol_adult = 2.0                # small counting tolerance
tol_ship = 2.0                 # shipping tolerance
SHIP_RESET_THRESHOLD = 5.0     # near-zero shipped = new season trigger
PENALTY_ALPHA = 10.0           # adult decrease weight
PENALTY_BETA = 12.0            # ship decrease weight
PENALTY_GAMMA = 0.1            # smoothness weight

# ------------------------------------------------------------
# PATHS
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir = project_root / "100_Data"

input_path  = data_dir / "csv_plotdata.csv"
output_path = data_dir / "11_twotrack_output.csv"
recent_path = data_dir / "csv_plotdata.csv"

# ------------------------------------------------------------
# LOAD
# ------------------------------------------------------------
if not input_path.exists():
    raise FileNotFoundError(f"❌ Missing input file: {input_path}")

df = pd.read_csv(input_path)
print(f"✅ Loaded {len(df):,} rows from {input_path.name}")

# Validate columns
for col in ["date_iso", "Adult_Total", "Live_Shipped"]:
    if col not in df.columns:
        raise ValueError(f"❌ Missing '{col}' column.")

df["date_iso"] = pd.to_datetime(df["date_iso"], errors="coerce")
df["Adult_Total"] = pd.to_numeric(df["Adult_Total"], errors="coerce").fillna(0.0)
df["Live_Shipped"] = pd.to_numeric(df["Live_Shipped"], errors="coerce").fillna(0.0)

group_cols = ["facility", "species", "Stock", "Stock_BO"]
df = df.sort_values(group_cols + ["date_iso"]).reset_index(drop=True)

# ------------------------------------------------------------
# HELPER: penalty function
# ------------------------------------------------------------
def penalty(track, adult, shipped):
    da = max(0, track["last_adult"] - adult)
    ds = max(0, track["last_ship"] - shipped)
    smooth = abs((adult - track["last_adult"]))
    return PENALTY_ALPHA * da + PENALTY_BETA * ds + PENALTY_GAMMA * smooth

# ------------------------------------------------------------
# CORE LOGIC
# ------------------------------------------------------------
def assign_tracks(group: pd.DataFrame) -> pd.DataFrame:
    g = group.sort_values("date_iso").copy()
    valsA = g["Adult_Total"].to_numpy(float)
    valsS = g["Live_Shipped"].to_numpy(float)
    dates = g["date_iso"].to_numpy()

    tracks = [
        {"last_adult": -np.inf, "last_ship": -np.inf, "last_date": pd.NaT, "count": 0},
        {"last_adult": -np.inf, "last_ship": -np.inf, "last_date": pd.NaT, "count": 0},
    ]

    out_track = np.zeros(len(g), dtype=int)
    long_gap_flag = np.zeros(len(g), dtype=int)
    newseason_flag = np.zeros(len(g), dtype=int)

    for i, (adult, ship, date) in enumerate(zip(valsA, valsS, dates)):
        prev_date = max(
            [t["last_date"] for t in tracks if pd.notna(t["last_date"])], default=pd.NaT
        )
        if pd.notna(date) and pd.notna(prev_date):
            day_gap = (pd.Timestamp(date) - pd.Timestamp(prev_date)).days
        else:
            day_gap = 0

        long_gap = day_gap > LONG_GAP_DAYS
        if long_gap:
            long_gap_flag[i] = 1

        # --- eligibility
        elig = []
        for t in tracks:
            okA = adult >= t["last_adult"] - tol_adult
            okS = ship >= t["last_ship"] - tol_ship
            elig.append(okA and okS)

        # --- choose track
        if sum(elig) == 1:
            k = 0 if elig[0] else 1
        elif sum(elig) == 2:
            cost0 = penalty(tracks[0], adult, ship)
            cost1 = penalty(tracks[1], adult, ship)
            k = 0 if cost0 <= cost1 else 1
        else:
            # Neither track fits
            if long_gap or ship <= SHIP_RESET_THRESHOLD:
                # New season flavor
                k = 0 if tracks[0]["last_ship"] <= tracks[1]["last_ship"] else 1
                newseason_flag[i] = 1
            else:
                # pick closest in shipped baseline
                diff0 = abs(ship - tracks[0]["last_ship"])
                diff1 = abs(ship - tracks[1]["last_ship"])
                k = 0 if diff0 <= diff1 else 1

        out_track[i] = k
        # update track
        tracks[k]["last_adult"] = max(tracks[k]["last_adult"], adult)
        tracks[k]["last_ship"] = max(tracks[k]["last_ship"], ship)
        tracks[k]["last_date"] = date
        tracks[k]["count"] += 1

    g["two_track_id"] = out_track
    g["long_gap_flag"] = long_gap_flag
    g["newseason_flag"] = newseason_flag
    return g

# ------------------------------------------------------------
# APPLY PER GROUP
# ------------------------------------------------------------
out = (
    df.groupby(group_cols, group_keys=False, sort=False)
      .apply(assign_tracks)
      .reset_index(drop=True)
)

# Map to "bio_year_fix" labels
# For each group, the track with larger median Live_Shipped is "older" (bio_year stays same)
# The smaller baseline is "new" (bio_year + 1)
bio_fix = []
for _, grp in out.groupby(group_cols, sort=False):
    if grp["Live_Shipped"].notna().any():
        med_ship = grp.groupby("two_track_id")["Live_Shipped"].median()
        if len(med_ship) == 2:
            old_track = med_ship.idxmax()
            new_track = 1 - old_track
        else:
            old_track = list(med_ship.index)[0]
            new_track = old_track
    else:
        old_track = 0
        new_track = 1
    base_bio = grp["bio_year"].min() if "bio_year" in grp else 1
    mapping = {old_track: base_bio, new_track: base_bio + 1}
    bio_fix.extend(grp["two_track_id"].map(mapping))

out["bio_year_fix"] = bio_fix

# ------------------------------------------------------------
# SAVE
# ------------------------------------------------------------
out.to_csv(output_path, index=False)
out.to_csv(recent_path, index=False)

print(f"✅ Dual-track assignment complete → {output_path}")
print(f"🔄 csv_plotdata.csv updated with bio_year_fix and diagnostics")