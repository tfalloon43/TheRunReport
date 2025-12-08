import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
from tkinter import Tk, Label, OptionMenu, StringVar, Button, messagebox

# ------------------------------------------------------------
# Load Data
# ------------------------------------------------------------
project_root = Path(__file__).resolve().parents[0]
data_dir     = project_root / "100_Data"

unify_path    = data_dir / "csv_unify_fishcounts.csv"
columbia_path = data_dir / "columbiadaily_raw.csv"
usgs_path     = data_dir / "USGS_flows.csv"
noaa_path     = data_dir / "NOAA_flows.csv"

df_unify    = pd.read_csv(unify_path)
df_columbia = pd.read_csv(columbia_path)

# Load USGS flows
if usgs_path.exists():
    df_usgs = pd.read_csv(usgs_path)
    if "timestamp" in df_usgs.columns:
        df_usgs["timestamp_dt"] = pd.to_datetime(df_usgs["timestamp"])
else:
    df_usgs = None

# Load NOAA flows
if noaa_path.exists():
    df_noaa = pd.read_csv(noaa_path)
    if "timestamp" in df_noaa.columns:
        df_noaa["timestamp_dt"] = pd.to_datetime(df_noaa["timestamp"])
else:
    df_noaa = None

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
DAM_RIVERS = {"Columbia River", "Snake River"}

# ------------------------------------------------------------
# GUI Setup
# ------------------------------------------------------------
root = Tk()
root.title("Fish Count & Flow Plotter")
root.geometry("520x520")

# --------------------------
# Widgets
# --------------------------
Label(root, text="Select River:", font=("Arial", 12)).pack(pady=5)
river_var = StringVar(root)

river_choices = sorted(df_unify["river"].unique().tolist() + list(DAM_RIVERS))
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

Label(root, text="Data to Plot:", font=("Arial", 12)).pack(pady=5)
plot_type_var = StringVar(root)
plot_type_var.set("Fish Counts")
plot_type_menu = OptionMenu(root, plot_type_var, "Fish Counts", "Flow")
plot_type_menu.pack()

Label(root, text="Select Flow Site (if available):", font=("Arial", 12)).pack(pady=5)
flow_site_var = StringVar(root)
flow_site_menu = OptionMenu(root, flow_site_var, "")
flow_site_menu.pack()

Label(root, text="Select Flow Window:", font=("Arial", 12)).pack(pady=5)
flow_window_var = StringVar(root)
flow_window_var.set("7d")
flow_window_menu = OptionMenu(root, flow_window_var, "7d", "30d", "1y")
flow_window_menu.pack()

# ------------------------------------------------------------
# DROPDOWN UPDATE LOGIC (USGS + NOAA)
# ------------------------------------------------------------
def update_dropdowns(*args):
    river = river_var.get()

    # -------------------------------
    # DAM DROPDOWN
    # -------------------------------
    if river in DAM_RIVERS:
        df = df_columbia[df_columbia["river"] == river]
        dam_list = sorted(df["dam_name"].unique().tolist())
    else:
        dam_list = [""]

    dam_menu["menu"].delete(0, "end")
    dam_var.set(dam_list[0])
    for d in dam_list:
        dam_menu["menu"].add_command(label=d, command=lambda v=d: dam_var.set(v))

    # -------------------------------
    # SPECIES DROPDOWN
    # -------------------------------
    if river in DAM_RIVERS:
        df_sp = df_columbia[df_columbia["river"] == river]
    else:
        df_sp = df_unify[df_unify["river"] == river]

    sp_list = sorted(df_sp["Species_Plot"].unique().tolist())
    if not sp_list:
        sp_list = ["No species"]

    species_menu["menu"].delete(0, "end")
    species_var.set(sp_list[0])
    for s in sp_list:
        species_menu["menu"].add_command(label=s, command=lambda v=s: species_var.set(v))

    # -------------------------------
    # FLOW SITE DROPDOWN (USGS + NOAA)
    # -------------------------------
    sites = []

    if df_usgs is not None:
        sites_usgs = (
            df_usgs[df_usgs["river"] == river]["site_name"]
            .dropna().unique().tolist()
        )
        sites += sites_usgs

    if df_noaa is not None:
        sites_noaa = (
            df_noaa[df_noaa["river"] == river]["site_name"]
            .dropna().unique().tolist()
        )
        sites += sites_noaa

    sites = sorted(set(sites))

    flow_site_menu["menu"].delete(0, "end")

    if sites:
        site_list = ["All Sites"] + sites
        flow_site_var.set(site_list[0])
        for s in site_list:
            flow_site_menu["menu"].add_command(label=s, command=lambda v=s: flow_site_var.set(v))
    else:
        flow_site_var.set("No flow data")
        flow_site_menu["menu"].add_command(
            label="No flow data",
            command=lambda v="No flow data": flow_site_var.set(v)
        )

river_var.trace("w", update_dropdowns)
plot_type_var.trace("w", update_dropdowns)

# ------------------------------------------------------------
# PLOTTING LOGIC
# ------------------------------------------------------------
def plot_flow(river, site_choice, window):
    """Unified FLOW plotting for both USGS and NOAA."""

    if df_usgs is None and df_noaa is None:
        messagebox.showinfo("No Flow Data", "No flow data files found.")
        return

    # Combine USGS + NOAA
    frames = []

    if df_usgs is not None:
        frames.append(df_usgs[df_usgs["river"] == river])

    if df_noaa is not None:
        frames.append(df_noaa[df_noaa["river"] == river])

    df_flow = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    if df_flow.empty:
        messagebox.showinfo("No Flow Data", f"No flow data for {river}.")
        return

    # Site filter
    if site_choice not in ("All Sites", "No flow data", ""):
        df_flow = df_flow[df_flow["site_name"] == site_choice]

    if df_flow.empty:
        messagebox.showinfo("No Flow Data", f"No flow data for site {site_choice}.")
        return

    # Window filter
    df_flow = df_flow[df_flow["window"] == window]

    if df_flow.empty:
        messagebox.showinfo("No Flow Data", f"No flow data for {window} window.")
        return

    # Ensure timestamp
    df_flow["timestamp_dt"] = pd.to_datetime(df_flow["timestamp"])

    df_flow = df_flow.sort_values("timestamp_dt")

    has_flow = "flow_cfs" in df_flow.columns
    has_stage = "stage_ft" in df_flow.columns

    plt.figure(figsize=(14, 7))
    ax1 = plt.gca()

    # FLOW — left axis
    if has_flow:
        ax1.plot(df_flow["timestamp_dt"], df_flow["flow_cfs"], linewidth=2, label="Flow (cfs)")
        ax1.set_ylabel("Flow (cfs)")

    # STAGE — right axis
    if has_stage:
        ax2 = ax1.twinx()
        ax2.plot(df_flow["timestamp_dt"], df_flow["stage_ft"], linewidth=2, linestyle="--", label="Stage (ft)")
        ax2.set_ylabel("Stage (ft)")
    else:
        ax2 = None

    title_site = site_choice if site_choice != "All Sites" else "All Sites"
    plt.title(f"{river} – {title_site} – Flow ({window})", fontsize=16)
    plt.xlabel("Date/Time")

    # Legend
    lines, labels = ax1.get_legend_handles_labels()
    if ax2:
        lines2, labels2 = ax2.get_legend_handles_labels()
        lines += lines2
        labels += labels2
    ax1.legend(lines, labels)

    plt.tight_layout()
    plt.show()


def plot_data():
    river   = river_var.get()
    species = species_var.get()
    site_choice = flow_site_var.get()
    window  = flow_window_var.get()
    mode    = plot_type_var.get()

    # FISH MODE
    if mode == "Fish Counts":
        if river in DAM_RIVERS:
            df = df_columbia[
                (df_columbia["river"] == river) &
                (df_columbia["dam_name"] == dam_var.get()) &
                (df_columbia["Species_Plot"] == species)
            ]
            if df.empty:
                messagebox.showinfo("No Data", "No fish data for this selection.")
                return

            plt.figure(figsize=(14, 7))
            plt.title(f"{river} – {dam_var.get()} – {species}")

            plt.plot(df["date_dt"], df["Ten_Year_Average_Daily_Count"], label="10-Year Avg", linewidth=2)
            plt.plot(df["date_dt"], df["Daily_Count_Last_Year"], label="Previous Year", linewidth=2)
            plt.plot(df["date_dt"], df["Daily_Count_Current_Year"], label="Current Year", linewidth=2)
            plt.legend()
            plt.tight_layout()
            plt.show()
            return

        # Non-dam rivers
        df = df_unify[
            (df_unify["river"] == river) &
            (df_unify["Species_Plot"] == species)
        ]

        plt.figure(figsize=(14, 7))
        plt.title(f"{river} – {species}")

        df10   = df[df["metric_type"] == "ten_year_avg"]
        dfprev = df[df["metric_type"] == "previous_year"]
        dfcur  = df[df["metric_type"] == "current_year"]

        plt.plot(df10["date_dt"], df10["value"], label="10-Year Avg")
        plt.plot(dfprev["date_dt"], dfprev["value"], label="Previous Year")
        plt.plot(dfcur["date_dt"], dfcur["value"], label="Current Year")

        plt.legend()
        plt.tight_layout()
        plt.show()
        return

    # FLOW MODE
    plot_flow(river, site_choice, window)


# Plot button
Button(root, text="Plot", command=plot_data,
       font=("Arial", 14), bg="lightblue").pack(pady=20)

update_dropdowns()
root.mainloop()