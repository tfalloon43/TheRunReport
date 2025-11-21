# 1_datapull.py
# ------------------------------------------------------------
# Step 1: Pull Columbia & Snake River daily counts (all dams/species)
# and build one unified long-format CSV.
#
# Output → 100_Data/columbiadaily_long.csv
# ------------------------------------------------------------

import requests
import pandas as pd
from io import StringIO
from pathlib import Path

print("🐟 Step 1: Pulling Columbia/Snake daily counts into unified long-format table...")

# ------------------------------------------------------------
# 65 FLAT CONFIG ENTRIES (you fill in urls)
# ------------------------------------------------------------
SOURCES = [

    # ============================
    # Columbia River – Bonneville Dam (5)
    # ============================
    { "dam": "Bonneville Dam", "river": "Columbia", "species": "Chinook",             "run": "All", "stock": "Adult", "url": "" },
    { "dam": "Bonneville Dam", "river": "Columbia", "species": "Coho",                "run": "All", "stock": "Adult", "url": "" },
    { "dam": "Bonneville Dam", "river": "Columbia", "species": "Steelhead",           "run": "All", "stock": "Adult", "url": "" },
    { "dam": "Bonneville Dam", "river": "Columbia", "species": "Unclipped Steelhead", "run": "All", "stock": "Adult", "url": "" },
    { "dam": "Bonneville Dam", "river": "Columbia", "species": "Sockeye",             "run": "All", "stock": "Adult", "url": "" },

    # ============================
    # The Dalles Dam (5)
    # ============================
    { "dam": "The Dalles Dam", "river": "Columbia", "species": "Chinook",             "run": "All", "stock": "Adult", "url": "" },
    { "dam": "The Dalles Dam", "river": "Columbia", "species": "Coho",                "run": "All", "stock": "Adult", "url": "" },
    { "dam": "The Dalles Dam", "river": "Columbia", "species": "Steelhead",           "run": "All", "stock": "Adult", "url": "" },
    { "dam": "The Dalles Dam", "river": "Columbia", "species": "Unclipped Steelhead", "run": "All", "stock": "Adult", "url": "" },
    { "dam": "The Dalles Dam", "river": "Columbia", "species": "Sockeye",             "run": "All", "stock": "Adult", "url": "" },

    # ============================
    # John Day Dam (5)
    # ============================
    { "dam": "John Day Dam", "river": "Columbia", "species": "Chinook",             "run": "All", "stock": "Adult", "url": "" },
    { "dam": "John Day Dam", "river": "Columbia", "species": "Coho",                "run": "All", "stock": "Adult", "url": "" },
    { "dam": "John Day Dam", "river": "Columbia", "species": "Steelhead",           "run": "All", "stock": "Adult", "url": "" },
    { "dam": "John Day Dam", "river": "Columbia", "species": "Unclipped Steelhead", "run": "All", "stock": "Adult", "url": "" },
    { "dam": "John Day Dam", "river": "Columbia", "species": "Sockeye",             "run": "All", "stock": "Adult", "url": "" },

    # ============================
    # McNary Dam (5)
    # ============================
    { "dam": "McNary Dam", "river": "Columbia", "species": "Chinook",             "run": "All", "stock": "Adult", "url": "" },
    { "dam": "McNary Dam", "river": "Columbia", "species": "Coho",                "run": "All", "stock": "Adult", "url": "" },
    { "dam": "McNary Dam", "river": "Columbia", "species": "Steelhead",           "run": "All", "stock": "Adult", "url": "" },
    { "dam": "McNary Dam", "river": "Columbia", "species": "Unclipped Steelhead", "run": "All", "stock": "Adult", "url": "" },
    { "dam": "McNary Dam", "river": "Columbia", "species": "Sockeye",             "run": "All", "stock": "Adult", "url": "" },

    # ============================
    # Priest Rapids Dam (5)
    # ============================
    { "dam": "Priest Rapids Dam", "river": "Columbia", "species": "Chinook",             "run": "All", "stock": "Adult", "url": "" },
    { "dam": "Priest Rapids Dam", "river": "Columbia", "species": "Coho",                "run": "All", "stock": "Adult", "url": "" },
    { "dam": "Priest Rapids Dam", "river": "Columbia", "species": "Steelhead",           "run": "All", "stock": "Adult", "url": "" },
    { "dam": "Priest Rapids Dam", "river": "Columbia", "species": "Unclipped Steelhead", "run": "All", "stock": "Adult", "url": "" },
    { "dam": "Priest Rapids Dam", "river": "Columbia", "species": "Sockeye",             "run": "All", "stock": "Adult", "url": "" },

    # ============================
    # Wanapum Dam (5)
    # ============================
    { "dam": "Wanapum Dam", "river": "Columbia", "species": "Chinook",             "run": "All", "stock": "Adult", "url": "" },
    { "dam": "Wanapum Dam", "river": "Columbia", "species": "Coho",                "run": "All", "stock": "Adult", "url": "" },
    { "dam": "Wanapum Dam", "river": "Columbia", "species": "Steelhead",           "run": "All", "stock": "Adult", "url": "" },
    { "dam": "Wanapum Dam", "river": "Columbia", "species": "Unclipped Steelhead", "run": "All", "stock": "Adult", "url": "" },
    { "dam": "Wanapum Dam", "river": "Columbia", "species": "Sockeye",             "run": "All", "stock": "Adult", "url": "" },

    # ============================
    # Rock Island Dam (5)
    # ============================
    { "dam": "Rock Island Dam", "river": "Columbia", "species": "Chinook",             "run": "All", "stock": "Adult", "url": "" },
    { "dam": "Rock Island Dam", "river": "Columbia", "species": "Coho",                "run": "All", "stock": "Adult", "url": "" },
    { "dam": "Rock Island Dam", "river": "Columbia", "species": "Steelhead",           "run": "All", "stock": "Adult", "url": "" },
    { "dam": "Rock Island Dam", "river": "Columbia", "species": "Unclipped Steelhead", "run": "All", "stock": "Adult", "url": "" },
    { "dam": "Rock Island Dam", "river": "Columbia", "species": "Sockeye",             "run": "All", "stock": "Adult", "url": "" },

    # ============================
    # Rocky Reach Dam (5)
    # ============================
    { "dam": "Rocky Reach Dam", "river": "Columbia", "species": "Chinook",             "run": "All", "stock": "Adult", "url": "" },
    { "dam": "Rocky Reach Dam", "river": "Columbia", "species": "Coho",                "run": "All", "stock": "Adult", "url": "" },
    { "dam": "Rocky Reach Dam", "river": "Columbia", "species": "Steelhead",           "run": "All", "stock": "Adult", "url": "" },
    { "dam": "Rocky Reach Dam", "river": "Columbia", "species": "Unclipped Steelhead", "run": "All", "stock": "Adult", "url": "" },
    { "dam": "Rocky Reach Dam", "river": "Columbia", "species": "Sockeye",             "run": "All", "stock": "Adult", "url": "" },

    # ============================
    # Wells Dam (5)
    # ============================
    { "dam": "Wells Dam", "river": "Columbia", "species": "Chinook",             "run": "All", "stock": "Adult", "url": "" },
    { "dam": "Wells Dam", "river": "Columbia", "species": "Coho",                "run": "All", "stock": "Adult", "url": "" },
    { "dam": "Wells Dam", "river": "Columbia", "species": "Steelhead",           "run": "All", "stock": "Adult", "url": "" },
    { "dam": "Wells Dam", "river": "Columbia", "species": "Unclipped Steelhead", "run": "All", "stock": "Adult", "url": "" },
    { "dam": "Wells Dam", "river": "Columbia", "species": "Sockeye",             "run": "All", "stock": "Adult", "url": "" },

    # ============================
    # Snake River – 4 dams × 5 species
    # ============================

    # Ice Harbor Dam
    { "dam": "Ice Harbor Dam", "river": "Snake", "species": "Chinook",             "run": "All", "stock": "Adult", "url": "" },
    { "dam": "Ice Harbor Dam", "river": "Snake", "species": "Coho",                "run": "All", "stock": "Adult", "url": "" },
    { "dam": "Ice Harbor Dam", "river": "Snake", "species": "Steelhead",           "run": "All", "stock": "Adult", "url": "" },
    { "dam": "Ice Harbor Dam", "river": "Snake", "species": "Unclipped Steelhead", "run": "All", "stock": "Adult", "url": "" },
    { "dam": "Ice Harbor Dam", "river": "Snake", "species": "Sockeye",             "run": "All", "stock": "Adult", "url": "" },

    # Lower Monumental Dam
    { "dam": "Lower Monumental Dam", "river": "Snake", "species": "Chinook",             "run": "All", "stock": "Adult", "url": "" },
    { "dam": "Lower Monumental Dam", "river": "Snake", "species": "Coho",                "run": "All", "stock": "Adult", "url": "" },
    { "dam": "Lower Monumental Dam", "river": "Snake", "species": "Steelhead",           "run": "All", "stock": "Adult", "url": "" },
    { "dam": "Lower Monumental Dam", "river": "Snake", "species": "Unclipped Steelhead", "run": "All", "stock": "Adult", "url": "" },
    { "dam": "Lower Monumental Dam", "river": "Snake", "species": "Sockeye",             "run": "All", "stock": "Adult", "url": "" },

    # Little Goose Dam
    { "dam": "Little Goose Dam", "river": "Snake", "species": "Chinook",             "run": "All", "stock": "Adult", "url": "" },
    { "dam": "Little Goose Dam", "river": "Snake", "species": "Coho",                "run": "All", "stock": "Adult", "url": "" },
    { "dam": "Little Goose Dam", "river": "Snake", "species": "Steelhead",           "run": "All", "stock": "Adult", "url": "" },
    { "dam": "Little Goose Dam", "river": "Snake", "species": "Unclipped Steelhead", "run": "All", "stock": "Adult", "url": "" },
    { "dam": "Little Goose Dam", "river": "Snake", "species": "Sockeye",             "run": "All", "stock": "Adult", "url": "" },

    # Lower Granite Dam
    { "dam": "Lower Granite Dam", "river": "Snake", "species": "Chinook",             "run": "All", "stock": "Adult", "url": "" },
    { "dam": "Lower Granite Dam", "river": "Snake", "species": "Coho",                "run": "All", "stock": "Adult", "url": "" },
    { "dam": "Lower Granite Dam", "river": "Snake", "species": "Steelhead",           "run": "All", "stock": "Adult", "url": "" },
    { "dam": "Lower Granite Dam", "river": "Snake", "species": "Unclipped Steelhead", "run": "All", "stock": "Adult", "url": "" },
    { "dam": "Lower Granite Dam", "river": "Snake", "species": "Sockeye",             "run": "All", "stock": "Adult", "url": "" },
]

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir     = project_root / "100_Data"
data_dir.mkdir(exist_ok=True)
output_path  = data_dir / "columbiadaily_long.csv"

# ------------------------------------------------------------
# Detect date column
# ------------------------------------------------------------
def detect_date_column(df):
    for col in df.columns:
        if str(col).strip().lower() == "date":
            return col
    return df.columns[0]

# ------------------------------------------------------------
# Main fetch loop
# ------------------------------------------------------------
frames = []

for src in SOURCES:
    dam     = src["dam"]
    river   = src["river"]
    species = src["species"]
    run     = src["run"]
    stock   = src["stock"]
    url     = src["url"]

    print(f"\n🌐 Fetching {dam} — {species} — URL: {url}")
    if not url:
        print("   ⚠️ No URL provided — skipping.")
        continue

    try:
        resp = requests.get(url, timeout=20)
        resp.raise_for_status()
    except Exception as e:
        print(f"   ❌ DOWNLOAD FAILED: {e}")
        continue

    try:
        df = pd.read_csv(StringIO(resp.text))
    except Exception as e:
        print(f"   ❌ PARSE FAILED: {e}")
        continue

    if df.empty:
        print("   ⚠️ EMPTY CSV — skipped.")
        continue

    date_col = detect_date_column(df)
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df = df.dropna(subset=[date_col])

    numeric_cols = df.select_dtypes(include="number").columns.tolist()
    if not numeric_cols:
        for c in df.columns:
            if c != date_col:
                df[c] = pd.to_numeric(df[c], errors="coerce")
        numeric_cols = df.select_dtypes(include="number").columns.tolist()

    if not numeric_cols:
        print("   ⚠️ No numeric data — skipping.")
        continue

    long_df = df.melt(
        id_vars=[date_col],
        value_vars=numeric_cols,
        var_name="metric_type",
        value_name="value",
    )

    long_df.rename(columns={date_col: "date"}, inplace=True)

    long_df["dam"]     = dam
    long_df["river"]   = river
    long_df["species"] = species
    long_df["run"]     = run
    long_df["stock"]   = stock

    long_df = long_df.dropna(subset=["value"])
    long_df = long_df[["date", "dam", "river", "species", "run", "stock", "metric_type", "value"]]

    print(f"   ✅ Added {len(long_df):,} rows")
    frames.append(long_df)

# ------------------------------------------------------------
# Save final dataset
# ------------------------------------------------------------
if not frames:
    raise RuntimeError("❌ No data collected from any source.")

final = pd.concat(frames, ignore_index=True)
final = final.sort_values(["river", "dam", "species", "date"]).reset_index(drop=True)

final.to_csv(output_path, index=False)

print("\n🎉 Unified Columbia/Snake dataset created!")
print(f"📊 Total rows: {len(final):,}")
print(f"📁 Saved → {output_path}")