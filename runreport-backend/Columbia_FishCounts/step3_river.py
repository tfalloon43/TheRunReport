"""
step3_river.py — IN-MEMORY TRANSFORM

Adds a 'river' column based on dam_code → river mapping
defined in lookup_maps.Columbia_or_Snake.

New behavior:
  ✔ Accepts a DataFrame as input
  ✔ Returns a DataFrame with a new 'river' column
  ✔ No filesystem access
  ✔ Works inside serverless ETL / Fly.io environment
"""

import pandas as pd
from pathlib import Path
from typing import Dict
import sys


# ------------------------------------------------------------
# Import lookup_maps.py from project root
# ------------------------------------------------------------
CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parents[0]  # runreport-backend/

sys.path.append(str(PROJECT_ROOT))

try:
    import lookup_maps
except Exception as e:
    raise RuntimeError(f"❌ Could not import lookup_maps.py: {e}")

if not hasattr(lookup_maps, "Columbia_or_Snake"):
    raise KeyError("❌ lookup_maps.py must define 'Columbia_or_Snake'")

DAM_TO_RIVER: Dict[str, str] = lookup_maps.Columbia_or_Snake


# ------------------------------------------------------------
# Main transform
# ------------------------------------------------------------
def add_river_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds a 'river' column based on dam_code.

    Parameters
    ----------
    df : pandas.DataFrame
        Must contain 'dam_code'.

    Returns
    -------
    pandas.DataFrame
        Copy of df with new 'river' column added.
    """
    if "dam_code" not in df.columns:
        raise ValueError("❌ Required column 'dam_code' is missing.")

    def map_river(code: str) -> str:
        return DAM_TO_RIVER.get(code, "Unknown")

    df2 = df.copy()
    df2["river"] = df2["dam_code"].apply(map_river)

    return df2


# ------------------------------------------------------------
# Local test
# ------------------------------------------------------------
if __name__ == "__main__":
    print("🔧 Testing add_river_column...\n")

    sample = pd.DataFrame({
        "dam_code": ["BON", "LGS", "XYZ"],  # includes one unknown
        "value": [1, 2, 3]
    })

    print(add_river_column(sample))