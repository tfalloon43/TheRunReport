"""
step4_reorg.py — IN-MEMORY VERSION

Reorganizes Columbia daily data.

New behavior:
  ✔ Accepts a DataFrame as input
  ✔ Drops unused columns
  ✔ Validates required fields
  ✔ Reorders into final normalized structure
  ✔ Returns a new DataFrame
  ✔ No CSV I/O
"""

import pandas as pd


def reorganize_daily_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean + reorder columns for Columbia daily data.

    Removes:
        - Dam
        - dam_code
        - Species
        - species_code

    Final expected columns:
        Dates
        river
        dam_name
        Species_Plot
        Daily_Count_Current_Year
        Daily_Count_Last_Year
        Ten_Year_Average_Daily_Count
    """

    # Columns that should be removed AFTER add_river_column()
    cols_to_drop = ["Dam", "dam_code", "Species", "species_code"]

    df2 = df.drop(columns=[c for c in cols_to_drop if c in df.columns], errors="ignore")

    # Required final columns
    desired_order = [
        "Dates",
        "river",
        "dam_name",
        "Species_Plot",
        "Daily_Count_Current_Year",
        "Daily_Count_Last_Year",
        "Ten_Year_Average_Daily_Count",
    ]

    # Validate presence
    missing = [c for c in desired_order if c not in df2.columns]
    if missing:
        raise ValueError(f"❌ Missing required columns for reorganization: {missing}")

    # Reorder in final ETL structure
    df2 = df2[desired_order]

    return df2


# ------------------------------------------------------------
# Optional local test
# ------------------------------------------------------------
if __name__ == "__main__":
    print("🔧 Testing reorganize_daily_data...\n")

    sample = pd.DataFrame({
        "Dates": ["01-01", "01-02"],
        "river": ["Columbia", "Snake"],
        "dam_name": ["Bonneville", "Ice Harbor"],
        "Species_Plot": ["Chinook", "Steelhead"],
        "Daily_Count_Current_Year": [100, 200],
        "Daily_Count_Last_Year": [90, 180],
        "Ten_Year_Average_Daily_Count": [110, 190],
        "Dam": ["BON", "IHR"],    # will be dropped
        "Species": ["CHAD", "ST"] # will be dropped
    })

    out = reorganize_daily_data(sample)
    print(out)