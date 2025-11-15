# 50_check_cowlitz_coho_wild.py
# ------------------------------------------------------------
# Step 50: Quick visual check — Cowlitz River Wild Coho
# Using unified weekly CSV instead of individual files.
# ------------------------------------------------------------

import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

print("📈 Step 50: Plotting full-year trend for Cowlitz River Wild Coho (Unified Dataset)...")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir     = project_root / "100_Data"
unified_path = data_dir / "weekly_unified_long.csv"

if not unified_path.exists():
    raise FileNotFoundError(f"❌ Missing unified dataset: {unified_path}")

# ------------------------------------------------------------
# Load unified dataset
# ------------------------------------------------------------
df = pd.read_csv(unified_path)
print(f"✅ Loaded {len(df):,} rows from weekly_unified_long.csv")

# ------------------------------------------------------------
# Target filter configuration
# ------------------------------------------------------------
identifier_target = "Cowlitz River - Coho"
category_target   = "basinfamily"
stock_target      = "H"  # wild
metric_10yr       = "ten_year_avg"
metric_cur        = "current_year"

# ------------------------------------------------------------
# Filter 10-year & current-year data
# ------------------------------------------------------------
df_10yr = df[
    (df["identifier"] == identifier_target) &
    (df["category_type"] == category_target) &
    (df["stock"] == stock_target) &
    (df["metric_type"] == metric_10yr)
].copy()

df_cur = df[
    (df["identifier"] == identifier_target) &
    (df["category_type"] == category_target) &
    (df["stock"] == stock_target) &
    (df["metric_type"] == metric_cur)
].copy()

if df_10yr.empty:
    raise ValueError("❌ No 10-year data found for target identifier.")

if df_cur.empty:
    raise ValueError("❌ No CURRENT-year data found for target identifier.")

# ------------------------------------------------------------
# Prepare time columns
# ------------------------------------------------------------
for d in [df_10yr, df_cur]:
    d["MM-DD"] = pd.to_datetime("2024-" + d["MM-DD"], errors="coerce")
    d.sort_values("MM-DD", inplace=True)
    d["value"] = pd.to_numeric(d["value"], errors="coerce").fillna(0)

# ------------------------------------------------------------
# Plot
# ------------------------------------------------------------
plt.figure(figsize=(12, 6))

# Blue = Ten Year Avg
plt.plot(
    df_10yr["MM-DD"], df_10yr["value"],
    color="steelblue", linewidth=2.5, label="10-Year Average"
)

# Red = Current Year
plt.plot(
    df_cur["MM-DD"], df_cur["value"],
    color="firebrick", linewidth=2.5, label="Current Year 2025"
)

# ------------------------------------------------------------
# Styling
# ------------------------------------------------------------
plt.title("Cowlitz River – Wild Coho\n10-Year Average vs 2025 (Weekly)", fontsize=15, pad=15)
plt.xlabel("Date", fontsize=12)
plt.ylabel("Fish per Week", fontsize=12)
plt.grid(alpha=0.3)
plt.legend(fontsize=11)
plt.tight_layout()

plt.show()

print("✅ Plot generated successfully from unified dataset.")