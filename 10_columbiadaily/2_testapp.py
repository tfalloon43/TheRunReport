# 2_testapp.py
# ------------------------------------------------------------
# Test app for Columbia/Snake daily counts (columbiadaily_raw.csv)
#
# Dropdowns:
#   • Dam      → dam_name
#   • Species  → species_name
#
# Always plots (for the chosen dam + species):
#   • Daily_Count_Current_Year
#   • Daily_Count_Last_Year
#   • Ten_Year_Average_Daily_Count
#
# Uses Tkinter + Matplotlib, same structure as 1_app.py.
# ------------------------------------------------------------

from pathlib import Path
from datetime import datetime

import pandas as pd
import tkinter as tk
from tkinter import ttk, messagebox

from matplotlib.figure import Figure
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import matplotlib.dates as mdates

# ------------------------------------------------------------
# Load data from CSV
# ------------------------------------------------------------
def load_data():
    project_root = Path(__file__).resolve().parents[1]
    data_dir = project_root / "100_Data"
    csv_path = data_dir / "columbiadaily_raw.csv"

    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found at: {csv_path}")

    df = pd.read_csv(csv_path)

    # Normalize column names (just in case)
    df.columns = [c.strip() for c in df.columns]

    # Ensure expected columns exist
    required_cols = [
        "Dates",
        "dam_name",
        "species_name",
        "Daily_Count_Current_Year",
        "Daily_Count_Last_Year",
        "Ten_Year_Average_Daily_Count",
    ]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in CSV: {missing}")

    # Clean up types
    df["dam_name"] = df["dam_name"].astype(str).str.strip()
    df["species_name"] = df["species_name"].astype(str).str.strip()

    # Parse dates (Dates is MM/DD, we tack on a dummy year like 2024)
    # This is only for plotting on a date axis; the year itself doesn't matter.
    df["Dates"] = df["Dates"].astype(str).str.strip()
    df["plot_date"] = pd.to_datetime(
        df["Dates"] + "/2024",
        format="%m/%d/%Y",
        errors="coerce",
    )

    # Coerce counts to numeric
    for col in [
        "Daily_Count_Current_Year",
        "Daily_Count_Last_Year",
        "Ten_Year_Average_Daily_Count",
    ]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Drop rows with no usable date
    df = df.dropna(subset=["plot_date"])

    return df


# ------------------------------------------------------------
# Tkinter Application
# ------------------------------------------------------------
class ColumbiaDailyApp:
    def __init__(self, root):
        self.root = root
        self.root.title("The Run Report – Columbia/Snake Daily Counts Viewer")
        self.root.geometry("1100x750")

        # Load data
        try:
            self.df = load_data()
        except Exception as e:
            messagebox.showerror("Error loading data", str(e))
            root.destroy()
            return

        # Unique dam & species lists
        self.dam_names = sorted(self.df["dam_name"].unique().tolist())
        self.dam_var = tk.StringVar()
        self.species_var = tk.StringVar()

        # Build UI
        self._build_controls()
        self._build_plot()

        if self.dam_names:
            self.dam_var.set(self.dam_names[0])
            self.on_dam_change()

    # --------------------------------------------------------
    # UI Components
    # --------------------------------------------------------
    def _build_controls(self):
        frame = ttk.Frame(self.root, padding=10)
        frame.pack(side=tk.TOP, fill=tk.X)

        # Dam dropdown
        ttk.Label(frame, text="Dam:").grid(row=0, column=0, padx=5)
        self.dam_cb = ttk.Combobox(
            frame,
            textvariable=self.dam_var,
            values=self.dam_names,
            state="readonly",
            width=35,
        )
        self.dam_cb.grid(row=0, column=1, padx=5)
        self.dam_cb.bind("<<ComboboxSelected>>", lambda e: self.on_dam_change())

        # Species dropdown
        ttk.Label(frame, text="Species:").grid(row=0, column=2, padx=5)
        self.species_cb = ttk.Combobox(
            frame,
            textvariable=self.species_var,
            values=[],
            state="readonly",
            width=35,
        )
        self.species_cb.grid(row=0, column=3, padx=5)

        # Plot button
        ttk.Button(frame, text="Plot", command=self.on_plot).grid(row=0, column=4, padx=10)

    def _build_plot(self):
        plot_frame = ttk.Frame(self.root)
        plot_frame.pack(side=tk.TOP, fill=tk.BOTH, expand=True)

        self.fig = Figure(figsize=(10, 6), dpi=100)
        self.ax = self.fig.add_subplot(111)
        self.canvas = FigureCanvasTkAgg(self.fig, master=plot_frame)
        self.canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)
        self.canvas.mpl_connect("motion_notify_event", self.on_hover)

        status_frame = ttk.Frame(self.root, padding=(10, 5))
        status_frame.pack(side=tk.TOP, fill=tk.X)
        self.status_var = tk.StringVar(
            value="Hover over chart to see the date under your cursor."
        )
        ttk.Label(status_frame, textvariable=self.status_var, anchor="w").pack(fill=tk.X)

    # --------------------------------------------------------
    # Event Handlers
    # --------------------------------------------------------
    def on_dam_change(self):
        dam = self.dam_var.get()
        if not dam:
            self.species_cb["values"] = []
            self.species_var.set("")
            return

        subset = self.df[self.df["dam_name"] == dam]
        species_list = sorted(subset["species_name"].unique().tolist())

        self.species_cb["values"] = species_list
        if species_list:
            self.species_var.set(species_list[0])

    def on_plot(self):
        dam = self.dam_var.get()
        species = self.species_var.get()

        if not dam or not species:
            messagebox.showinfo("Missing Fields", "Select dam and species.")
            return

        subset = self.df[
            (self.df["dam_name"] == dam) & (self.df["species_name"] == species)
        ].copy()

        if subset.empty:
            messagebox.showinfo("No data", "No records found for that dam/species.")
            return

        subset = subset.sort_values("plot_date")

        # Clear previous plot
        self.ax.clear()

        # Plot the three lines
        if not subset["Daily_Count_Current_Year"].dropna().empty:
            self.ax.plot(
                subset["plot_date"],
                subset["Daily_Count_Current_Year"],
                label="Current Year",
                linewidth=2.5,
            )

        if not subset["Daily_Count_Last_Year"].dropna().empty:
            self.ax.plot(
                subset["plot_date"],
                subset["Daily_Count_Last_Year"],
                label="Last Year",
                linewidth=2,
            )

        if not subset["Ten_Year_Average_Daily_Count"].dropna().empty:
            self.ax.plot(
                subset["plot_date"],
                subset["Ten_Year_Average_Daily_Count"],
                label="10-Year Avg",
                linewidth=2,
                linestyle="--",
            )

        # Labels / formatting
        self.ax.set_title(f"{dam} — {species}", fontsize=14, pad=12)
        self.ax.set_xlabel("Date (MM-DD)")
        self.ax.set_ylabel("Daily Adult Count")
        self.ax.grid(alpha=0.3)
        self.ax.legend()

        # Format x-axis dates nicely
        self.ax.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d"))
        self.fig.autofmt_xdate(rotation=45)

        self.canvas.draw()

    def on_hover(self, event):
        if not hasattr(self, "status_var"):
            return

        if event.inaxes != self.ax or event.xdata is None:
            self.status_var.set("Date: —")
            return

        try:
            dt = mdates.num2date(event.xdata)
        except Exception:
            self.status_var.set("Date: —")
            return

        self.status_var.set(f"Date: {dt.strftime('%b %d')}")

# ------------------------------------------------------------
# Entrypoint
# ------------------------------------------------------------
if __name__ == "__main__":
    root = tk.Tk()
    app = ColumbiaDailyApp(root)
    root.mainloop()