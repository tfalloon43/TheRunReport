# 1_datapull.py
# ------------------------------------------------------------
# Pulls Bonneville Dam Chinook daily count CSV directly from FPC
# and saves it as 100_Data/columbiacounts.csv
# ------------------------------------------------------------

import pandas as pd
import requests
from pathlib import Path
from io import StringIO   # <<— FIXED

print("🐟 Pulling Bonneville Chinook Daily Counts...")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir     = project_root / "100_Data"
data_dir.mkdir(exist_ok=True)

output_path = data_dir / "columbiacounts.csv"

# ------------------------------------------------------------
# FPC CSV URL (direct link from the HTML you uploaded)
# ------------------------------------------------------------
CSV_URL = "https://www.fpc.org/DataReqs/web/apps/adultsalmon/DA4691f0ac80ce00.csv"

print(f"🌐 Fetching CSV from: {CSV_URL}")

try:
    response = requests.get(CSV_URL, timeout=20)
    response.raise_for_status()
except Exception as e:
    raise RuntimeError(f"❌ Failed to download CSV: {e}")

print("📥 Download successful — parsing CSV...")

# ------------------------------------------------------------
# Load into DataFrame (FIXED)
# ------------------------------------------------------------
try:
    df = pd.read_csv(StringIO(response.text))
except Exception as e:
    raise RuntimeError(f"❌ Could not parse CSV: {e}")

print(f"✅ Loaded {len(df):,} rows and {len(df.columns)} columns")

# ------------------------------------------------------------
# Save locally
# ------------------------------------------------------------
df.to_csv(output_path, index=False)
print(f"💾 Saved → {output_path}")

print("🎣 Bonneville Chinook data pull complete.")