"""
step2_species_plot.py — IN-MEMORY TRANSFORMATION

Creates a clean Species_Plot column based on species_name.

This step:
  ✔ Accepts a DataFrame as input
  ✔ Adds Species_Plot without modifying the original DataFrame
  ✔ Returns a NEW DataFrame (pure functional transform)
  ✔ No CSV writes, no filesystem interaction
"""

import pandas as pd


# ------------------------------------------------------------
# Helper
# ------------------------------------------------------------
def clean_species_name(name: str) -> str:
    """
    Remove trailing ' Adult' from species names.
    Example: 'Chinook Adult' → 'Chinook'
    """
    if not isinstance(name, str):
        return name
    name = name.strip()
    return name[:-6].rstrip() if name.endswith(" Adult") else name


# ------------------------------------------------------------
# Main transformation function
# ------------------------------------------------------------
def add_species_plot(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds a Species_Plot column based on species_name.

    Parameters
    ----------
    df : pandas.DataFrame
        Expected to contain column 'species_name'.

    Returns
    -------
    pandas.DataFrame
        A new DataFrame with Species_Plot added.
    """
    if "species_name" not in df.columns:
        raise ValueError("❌ Missing required column: 'species_name'")

    df2 = df.copy()
    df2["Species_Plot"] = df2["species_name"].apply(clean_species_name)
    return df2


# ------------------------------------------------------------
# Standalone testing
# ------------------------------------------------------------
if __name__ == "__main__":
    print("🔧 Testing add_species_plot()...\n")

    sample = pd.DataFrame({
        "species_name": ["Chinook Adult", "Steelhead", "Coho Adult", "Sockeye"]
    })

    print(add_species_plot(sample))