# 11_Stock_BO.py
# ------------------------------------------------------------
# Build Stock_BO in two phases:
#   Phase 1 → copy TL4 for all rows with a date
#   Phase 2 → if a row has stock_presence_lower, append that row's TL6
#              to the *previous row's* Stock_BO (always append)
# After both phases → clean out any 'nan', 'None', or 'NaN' text artifacts
# ------------------------------------------------------------

import os
import pandas as pd

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
base_dir = os.path.dirname(os.path.abspath(__file__))
input_path = os.path.join(base_dir, "csv_recent.csv")
output_path = os.path.join(base_dir, "11_Stock_BO_output.csv")
recent_path = os.path.join(base_dir, "csv_recent.csv")

print("🏗️  Step 11: Building Stock_BO...")

# ------------------------------------------------------------
# Load data
# ------------------------------------------------------------
if not os.path.exists(input_path):
    raise FileNotFoundError(f"Missing {input_path} — run previous step first.")
df = pd.read_csv(input_path)

# Ensure columns exist
for col in ["date", "TL4", "TL6", "stock_presence_lower"]:
    if col not in df.columns:
        df[col] = ""

# ------------------------------------------------------------
# Phase 1 — copy TL4 where date exists
# ------------------------------------------------------------
df["Stock_BO"] = df.apply(
    lambda r: str(r["TL4"]).strip()
    if isinstance(r["date"], str) and r["date"].strip() and r["date"].lower() != "nan"
    else "",
    axis=1,
)
print(f"✅ Phase 1 complete: {df['Stock_BO'].astype(bool).sum()} rows initialized from TL4.")

# ------------------------------------------------------------
# Phase 2 — append TL6 from lower rows with stock_presence_lower
# ------------------------------------------------------------
for i in range(1, len(df)):
    row = df.iloc[i]
    lower_val = str(row.get("stock_presence_lower", "")).strip()
    tl6_val = str(row.get("TL6", "")).strip()

    # Clean out invalid TL6 values before appending
    if lower_val and tl6_val.lower() not in ("", "nan", "none"):
        prev_idx = i - 1
        if prev_idx >= 0:
            current_val = str(df.at[prev_idx, "Stock_BO"]).strip()
            new_val = f"{current_val} {tl6_val}".strip()
            new_val = " ".join(new_val.split())  # normalize spacing
            # Remove any literal 'nan' tokens in middle of string
            new_val = new_val.replace(" nan", "").replace("nan", "").strip()
            df.at[prev_idx, "Stock_BO"] = new_val

print("✅ Phase 2 complete: appended TL6 values to previous rows where stock_presence_lower exists.")

# ------------------------------------------------------------
# Final cleanup
# ------------------------------------------------------------
df["Stock_BO"] = (
    df["Stock_BO"]
    .astype(str)
    .replace(["nan", "NaN", "None"], "")
    .str.replace(r"\bnan\b", "", regex=True)
    .str.strip()
)
df.loc[df["Stock_BO"].eq(""), "Stock_BO"] = ""  # true blanks only

# ------------------------------------------------------------
# Save outputs
# ------------------------------------------------------------
df.to_csv(output_path, index=False)
df.to_csv(recent_path, index=False)

# ------------------------------------------------------------
# Report
# ------------------------------------------------------------
total_rows = len(df)
populated = df["Stock_BO"].astype(str).str.strip().ne("").sum()
print(f"✅ Stock_BO construction complete → {output_path}")
print(f"🔄 csv_recent.csv updated with Stock_BO column")
print(f"📊 {populated} of {total_rows} rows populated with Stock_BO")