import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
from tkinter import Tk, Label, OptionMenu, StringVar, Button

# ------------------------------------------------------------
# Load Data
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[0]
data_dir     = project_root / "100_Data"

unify_path   = data_dir / "csv_unify_fishcounts.csv"
columbia_path = data_dir / "columbiadaily_raw.csv"

df_unify = pd.read_csv(unify_path)
df_columbia = pd.read_csv(columbia_path)

# ------------------------------------------------------------
# Normalize date formats
# ------------------------------------------------------------
df_unify["date_dt"] = pd.to_datetime("2024-" + df_unify["MM-DD"])
df_unify = df_unify.sort_values("date_dt")

df_columbia["date_dt"] = pd.to_datetime(
    "2024-" + df_columbia["Dates"].astype(str).str.replace("/", "-")
)
df_columbia = df_columbia.sort_values("date_dt")

# ------------------------------------------------------------
# Constants
# ------------------------------------------------------------
DAM_RIVERS = {"Columbia River", "Snake River"}   # NEW LOGIC

# ------------------------------------------------------------
# GUI Setup
# ------------------------------------------------------------
root = Tk()
root.title("Fish Count Plotter")
root.geometry("460x350")

Label(root, text="Select River:", font=("Arial", 12)).pack(pady=5)

# River dropdown
river_var = StringVar(root)
river_choices = sorted(df_unify["river"].unique().tolist() + list(DAM_RIVERS))
river_var.set(river_choices[0])

river_menu = OptionMenu(root, river_var, *river_choices)
river_menu.pack()

# Dam Dropdown (Columbia OR Snake only)
Label(root, text="Select Dam (Columbia / Snake Only):", font=("Arial", 12)).pack(pady=5)
dam_var = StringVar(root)
dam_menu = OptionMenu(root, dam_var, "")
dam_menu.pack()

# Species Dropdown
Label(root, text="Select Species:", font=("Arial", 12)).pack(pady=5)
species_var = StringVar(root)
species_menu = OptionMenu(root, species_var, "")
species_menu.pack()

# ------------------------------------------------------------
# Update dropdowns based on river selection
# ------------------------------------------------------------
def update_dropdowns(*args):
    river = river_var.get()

    # -----------------------------------
    # 1. DAM DROPDOWN LOGIC (UPDATED)
    # -----------------------------------
    if river in DAM_RIVERS:
        df = df_columbia[df_columbia["river"] == river]
        dam_list = sorted(df["dam_name"].unique())

        if len(dam_list) == 0:
            dam_list = ["No dams available"]

        dam_var.set(dam_list[0])
        dam_menu["menu"].delete(0, "end")
        for d in dam_list:
            dam_menu["menu"].add_command(
                label=d, command=lambda value=d: dam_var.set(value)
            )
    else:
        # Hide dam option for non-dam rivers
        dam_var.set("")
        dam_menu["menu"].delete(0, "end")
        dam_menu["menu"].add_command(
            label="", command=lambda value="": dam_var.set("")
        )

    # -----------------------------------
    # 2. SPECIES DROPDOWN
    # -----------------------------------
    if river in DAM_RIVERS:
        df = df_columbia[df_columbia["river"] == river]
    else:
        df = df_unify[df_unify["river"] == river]

    sp_list = sorted(df["Species_Plot"].unique())
    species_var.set(sp_list[0])
    species_menu["menu"].delete(0, "end")
    for s in sp_list:
        species_menu["menu"].add_command(
            label=s, command=lambda value=s: species_var.set(value)
        )

river_var.trace("w", update_dropdowns)

# ------------------------------------------------------------
# Plotting Logic
# ------------------------------------------------------------
def plot_data():
    river = river_var.get()
    species = species_var.get()

    # --------------------------------------------------------
    # Columbia & Snake River Case (Has dams)
    # --------------------------------------------------------
    if river in DAM_RIVERS:
        dam = dam_var.get()
        df = df_columbia[
            (df_columbia["river"] == river) &
            (df_columbia["dam_name"] == dam) &
            (df_columbia["Species_Plot"] == species)
        ].copy()

        dates = df["date_dt"]

        plt.figure(figsize=(14,7))
        plt.title(f"{river} – {dam} – {species}", fontsize=16)

        plt.plot(dates, df["Ten_Year_Average_Daily_Count"], color="blue", label="10-Year Average", linewidth=2)
        plt.plot(dates, df["Daily_Count_Last_Year"], color="#ff9999", label="Previous Year", linewidth=2)
        plt.plot(dates, df["Daily_Count_Current_Year"], color="green", label="Current Year", linewidth=2)

        plt.xlabel("Date")
        plt.ylabel("Fish Count")
        plt.legend()
        plt.tight_layout()
        plt.show()
        return

    # --------------------------------------------------------
    # All OTHER non-dam rivers (Unified DB)
    # --------------------------------------------------------
    df = df_unify[
        (df_unify["river"] == river) &
        (df_unify["Species_Plot"] == species)
    ]

    df10   = df[df["metric_type"] == "ten_year_avg"].sort_values("date_dt")
    dfprev = df[df["metric_type"] == "previous_year"].sort_values("date_dt")
    dfcur  = df[df["metric_type"] == "current_year"].sort_values("date_dt")

    plt.figure(figsize=(14,7))
    plt.title(f"{river} – {species}", fontsize=16)

    plt.plot(df10["date_dt"], df10["value"], color="blue", label="10-Year Average", linewidth=2)
    plt.plot(dfprev["date_dt"], dfprev["value"], color="#ff9999", label="Previous Year", linewidth=2)
    plt.plot(dfcur["date_dt"], dfcur["value"], color="green", label="Current Year", linewidth=2)

    plt.xlabel("Date")
    plt.ylabel("Fish Count")
    plt.legend()
    plt.tight_layout()
    plt.show()

# ------------------------------------------------------------
# Plot button
# ------------------------------------------------------------
Button(root, text="Plot", command=plot_data,
       font=("Arial", 14), bg="lightblue").pack(pady=20)

update_dropdowns()
root.mainloop()