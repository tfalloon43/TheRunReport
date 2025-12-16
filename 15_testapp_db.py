"""
15_testapp_db.py
------------------------------------------------------------
Lightweight Tkinter plotter that reads fish counts directly
from local.db instead of CSVs.

- Escapement rivers: EscapementReport_PlotData
- Columbia/Snake dams: Columbia_FishCounts

Flows are ignored in this DB-backed version.
"""

import sqlite3
from pathlib import Path
from tkinter import Tk, Label, OptionMenu, StringVar, Button, messagebox

import matplotlib.pyplot as plt
import pandas as pd

# ------------------------------------------------------------
# Load data from DB
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parent
db_path = project_root / "runreport-backend" / "0_db" / "local.db"

if not db_path.exists():
    raise FileNotFoundError(f"local.db not found at {db_path}")

conn = sqlite3.connect(db_path)

df_esc = pd.read_sql("SELECT * FROM EscapementReport_PlotData", conn)
df_col = pd.read_sql("SELECT * FROM Columbia_FishCounts", conn)

conn.close()

# Normalize dates
if "MM-DD" in df_esc.columns:
    df_esc["date_dt"] = pd.to_datetime("2024-" + df_esc["MM-DD"], errors="coerce")
else:
    df_esc["date_dt"] = pd.NaT

# Columbia/Snake cleaning and date parsing (Dates are MM/DD)
for required_col in [
    "Ten_Year_Average_Daily_Count",
    "Daily_Count_Last_Year",
    "Daily_Count_Current_Year",
    "Dates",
    "dam_name",
    "river",
    "Species_Plot",
]:
    if required_col not in df_col.columns:
        raise ValueError(f"Missing required column in Columbia_FishCounts: {required_col}")

df_col["river"] = df_col["river"].astype(str).str.strip()
df_col["dam_name"] = df_col["dam_name"].astype(str).str.strip()
df_col["Species_Plot"] = df_col["Species_Plot"].astype(str).str.strip()
df_col["MM-DD"] = df_col["Dates"].astype(str).str.replace("/", "-", regex=False)
df_col["date_dt"] = pd.to_datetime("2024-" + df_col["MM-DD"], errors="coerce")

for c in ["Ten_Year_Average_Daily_Count", "Daily_Count_Last_Year", "Daily_Count_Current_Year"]:
    df_col[c] = pd.to_numeric(df_col[c], errors="coerce")

df_esc = df_esc.sort_values("date_dt")
df_col = df_col.sort_values("date_dt")

ten_col = "Ten_Year_Average_Daily_Count"
prev_col = "Daily_Count_Last_Year"
cur_col = "Daily_Count_Current_Year"

# ------------------------------------------------------------
# Constants
# ------------------------------------------------------------
DAM_RIVERS = {"Columbia River", "Snake River"}

# ------------------------------------------------------------
# GUI Setup
# ------------------------------------------------------------
root = Tk()
root.title("Fish Count Plotter (DB)")
root.geometry("520x500")

# --------------------------
# Widgets
# --------------------------
Label(root, text="Select River:", font=("Arial", 12)).pack(pady=5)
river_var = StringVar(root)

river_choices = sorted(set(df_esc.get("river", pd.Series()).dropna().unique().tolist()) | set(df_col.get("river", pd.Series()).dropna().unique().tolist()))
if not river_choices:
    river_choices = [""]
river_var.set(river_choices[0])
OptionMenu(root, river_var, *river_choices).pack()

Label(root, text="Select Dam (Columbia / Snake Only):", font=("Arial", 12)).pack(pady=5)
dam_var = StringVar(root)
dam_menu = OptionMenu(root, dam_var, "")
dam_menu.pack()

Label(root, text="Select Species:", font=("Arial", 12)).pack(pady=5)
species_var = StringVar(root)
species_menu = OptionMenu(root, species_var, "")
species_menu.pack()

# ------------------------------------------------------------
# DROPDOWN UPDATE LOGIC
# ------------------------------------------------------------
def update_dropdowns(*args):
    river = river_var.get()

    # DAM DROPDOWN
    if river in DAM_RIVERS:
        df = df_col[df_col["river"] == river]
        dam_list = sorted(df["dam_name"].dropna().unique().tolist())
    else:
        dam_list = [""]

    dam_menu["menu"].delete(0, "end")
    dam_var.set(dam_list[0] if dam_list else "")
    for d in dam_list:
        dam_menu["menu"].add_command(label=d, command=lambda v=d: dam_var.set(v))

    # SPECIES DROPDOWN
    if river in DAM_RIVERS:
        df_sp = df_col[df_col["river"] == river]
    else:
        df_sp = df_esc[df_esc.get("river", "") == river]

    sp_list = sorted(df_sp.get("Species_Plot", pd.Series()).dropna().unique().tolist())
    if not sp_list:
        sp_list = ["No species"]

    species_menu["menu"].delete(0, "end")
    species_var.set(sp_list[0])
    for s in sp_list:
        species_menu["menu"].add_command(label=s, command=lambda v=s: species_var.set(v))


river_var.trace("w", update_dropdowns)

# ------------------------------------------------------------
# PLOTTING LOGIC
# ------------------------------------------------------------
def plot_data():
    river = river_var.get()
    species = species_var.get()

    if river in DAM_RIVERS:
        df = df_col[
            (df_col["river"] == river)
            & (df_col["dam_name"] == dam_var.get())
            & (df_col["Species_Plot"] == species)
        ]
        if df.empty:
            messagebox.showinfo("No Data", "No fish data for this selection.")
            return

        plt.figure(figsize=(14, 7))
        plt.title(f"{river} – {dam_var.get()} – {species}")

        plt.plot(df["date_dt"], df[ten_col], label="10-Year Avg", linewidth=2)
        plt.plot(df["date_dt"], df[prev_col], label="Previous Year", linewidth=2)
        plt.plot(df["date_dt"], df[cur_col], label="Current Year", linewidth=2)
        plt.legend()
        plt.tight_layout()
        plt.show()
        return

    # Non-dam rivers (escapement)
    df = df_esc[
        (df_esc.get("river", "") == river)
        & (df_esc.get("Species_Plot", "") == species)
    ]

    if df.empty:
        messagebox.showinfo("No Data", "No fish data for this selection.")
        return

    plt.figure(figsize=(14, 7))
    plt.title(f"{river} – {species}")

    if "10_year" in df.columns:
        plt.plot(df["date_dt"], df["10_year"], label="10-Year Avg")
    if "previous_year" in df.columns:
        plt.plot(df["date_dt"], df["previous_year"], label="Previous Year")
    if "current_year" in df.columns:
        plt.plot(df["date_dt"], df["current_year"], label="Current Year")

    plt.legend()
    plt.tight_layout()
    plt.show()


# Plot button
Button(root, text="Plot", command=plot_data,
       font=("Arial", 14), bg="lightblue").pack(pady=20)

update_dropdowns()
root.mainloop()
