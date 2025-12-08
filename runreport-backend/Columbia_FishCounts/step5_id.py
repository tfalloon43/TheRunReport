"""
step5_id.py — IN-MEMORY VERSION

Adds:
  • Auto-incrementing ID column
  • Numeric type conversion for key count fields

New behavior:
  ✔ Accepts a DataFrame
  ✔ Returns a transformed DataFrame
  ✔ No file I/O
"""

import pandas as pd


def add_id_and_convert_numeric(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add an 'id' column as the FIRST column and convert numeric fields.

    Parameters
    ----------
    df : DataFrame
        Must contain:
          - Daily_Count_Current_Year
          - Daily_Count_Last_Year
          - Ten_Year_Average_Daily_Count

    Returns
    -------
    DataFrame with:
      - id column (1..N)
      - numeric columns enforced properly
    """

    df2 = df.copy()

    # ------------------------------------------------------------
    # 1) Insert ID column at position 0
    # ------------------------------------------------------------
    df2.insert(0, "id", range(1, len(df2) + 1))

    # ------------------------------------------------------------
    # 2) Convert numeric columns
    # ------------------------------------------------------------
    numeric_cols = [
        "Daily_Count_Current_Year",
        "Daily_Count_Last_Year",
        "Ten_Year_Average_Daily_Count",
    ]

    missing = [c for c in numeric_cols if c not in df2.columns]
    if missing:
        raise ValueError(f"❌ Missing required numeric columns: {missing}")

    for col in numeric_cols:
        df2[col] = pd.to_numeric(df2[col], errors="coerce")

    return df2


# ------------------------------------------------------------
# Optional debugging / local test
# ------------------------------------------------------------
if __name__ == "__main__":
    print("🔧 Testing add_id_and_convert_numeric...\n")

    sample = pd.DataFrame({
        "Dates": ["01-01", "01-02"],
        "river": ["Columbia", "Snake"],
        "dam_name": ["Bonneville", "Ice Harbor"],
        "Species_Plot": ["Chinook", "Steelhead"],
        "Daily_Count_Current_Year": ["100", "200"],
        "Daily_Count_Last_Year": ["90", "180"],
        "Ten_Year_Average_Daily_Count": ["110", "190"],
    })

    out = add_id_and_convert_numeric(sample)
    print(out)