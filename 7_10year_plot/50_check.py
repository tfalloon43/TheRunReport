# 50_check.py
# ------------------------------------------------------------
# Step 50: Quick visual check of weekly 10-year averages
#
# Plots:
#   • X-axis → MM-DD (time)
#   • Y-axis → weekly average values
#
# Source file: hatchfamily_h_weekly.csv
# Target column: "Cowlitz Salmon Hatchery - Coho"
#
# Output:
#   • A simple Matplotlib line chart
# ------------------------------------------------------------

import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

print("📈 Step 50: Plotting weekly trend for Cowlitz Salmon Hatchery - Coho...")

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[1]
data_dir = project_root / "100_Data"
input_path = data_dir / "hatchfamily_h_weekly.csv"

# ------------------------------------------------------------
# Load data
# ------------------------------------------------------------
if not input_path.exists():
    raise FileNotFoundError(f"❌ Missing file: {input_path}")

df = pd.read_csv(input_path)
print(f"✅ Loaded {len(df):,} rows from {input_path.name}")

target_col = "Cowlitz Salmon Hatchery - Coho"
if target_col not in df.columns:
    raise ValueError(f"❌ Column not found: '{target_col}'")

# ------------------------------------------------------------
# Prepare data
# ------------------------------------------------------------
# Ensure chronological order and numeric values
df["MM-DD"] = pd.to_datetime("2024-" + df["MM-DD"], errors="coerce")
df = df.sort_values("MM-DD")
df[target_col] = pd.to_numeric(df[target_col], errors="coerce").fillna(0)

# ------------------------------------------------------------
# Plot
# ------------------------------------------------------------
plt.figure(figsize=(12, 6))
plt.plot(df["MM-DD"], df[target_col], linewidth=2, label=target_col, color="steelblue")

plt.title("Cowlitz Salmon Hatchery - Coho (Weekly 10-Year Average)", fontsize=14, pad=15)
plt.xlabel("Date", fontsize=12)
plt.ylabel("Fish per Week (10-Year Avg)", fontsize=12)
plt.grid(alpha=0.3)
plt.tight_layout()
plt.legend()

# ------------------------------------------------------------
# Show plot
# ------------------------------------------------------------
plt.show()

print("✅ Plot generated successfully.")