# 1_app.py — updated to plot all H & W lines on same chart
# ------------------------------------------------------------
# Lines shown:
#   H 10-year avg     → faint blue
#   H current-year    → solid blue
#   W 10-year avg     → faint red
#   W current-year    → solid red
#   U 10-year avg     → faint green
#   U current-year    → solid green
#
# You select only:
#   • category_type
#   • identifier
#
# It automatically plots all stock/metric combinations.
# ------------------------------------------------------------

import sqlite3
from pathlib import Path
from datetime import datetime

import pandas as pd
import tkinter as tk
from tkinter import ttk, messagebox

from matplotlib.figure import Figure
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg

CURRENT_YEAR = datetime.now().year
STOCK_CONFIGS = {
    "H": {"color": "blue", "name": "Hatchery"},
    "W": {"color": "red", "name": "Wild"},
    "U": {"color": "green", "name": "U Fish"},
}


# ------------------------------------------------------------
# Load data from SQLite
# ------------------------------------------------------------
def load_data():
    project_root = Path(__file__).resolve().parents[1]
    data_dir = project_root / "100_Data"
    db_path = data_dir / "pdf_data.sqlite"

    if not db_path.exists():
        raise FileNotFoundError(f"SQLite database not found at: {db_path}")

    conn = sqlite3.connect(db_path)
    query = """
        SELECT MM_DD, identifier, value, category_type, stock, metric_type
        FROM z5_10y_averages_currentyear
    """
    df = pd.read_sql_query(query, conn)
    conn.close()

    df["MM_DD"] = df["MM_DD"].astype(str)
    df["identifier"] = df["identifier"].astype(str).str.strip()
    df["category_type"] = df["category_type"].astype(str).str.strip()
    df["stock"] = df["stock"].astype(str).str.strip().str.upper()
    df["metric_type"] = df["metric_type"].astype(str).str.strip()
    df["value"] = pd.to_numeric(df["value"], errors="coerce").fillna(0.0)

    return df


# ------------------------------------------------------------
# Tkinter Application
# ------------------------------------------------------------
class RunReportApp:
    def __init__(self, root):
        self.root = root
        self.root.title("The Run Report – Multi-Stock Weekly Viewer")
        self.root.geometry("1100x750")

        # Load data
        try:
            self.df = load_data()
        except Exception as e:
            messagebox.showerror("Error loading data", str(e))
            root.destroy()
            return

        self.category_types = sorted(self.df["category_type"].unique().tolist())
        self.category_var = tk.StringVar()
        self.identifier_var = tk.StringVar()

        self._build_controls()
        self._build_plot()

        if self.category_types:
            self.category_var.set(self.category_types[0])
            self.on_category_change()

    # --------------------------------------------------------
    # UI Components
    # --------------------------------------------------------
    def _build_controls(self):
        frame = ttk.Frame(self.root, padding=10)
        frame.pack(side=tk.TOP, fill=tk.X)

        ttk.Label(frame, text="Category:").grid(row=0, column=0, padx=5)
        self.category_cb = ttk.Combobox(
            frame, textvariable=self.category_var,
            values=self.category_types, state="readonly", width=25
        )
        self.category_cb.grid(row=0, column=1, padx=5)
        self.category_cb.bind("<<ComboboxSelected>>", lambda e: self.on_category_change())

        ttk.Label(frame, text="Identifier:").grid(row=0, column=2, padx=5)
        self.identifier_cb = ttk.Combobox(
            frame, textvariable=self.identifier_var,
            values=[], state="readonly", width=45
        )
        self.identifier_cb.grid(row=0, column=3, padx=5)

        ttk.Button(frame, text="Plot", command=self.on_plot).grid(row=0, column=4, padx=10)

    def _build_plot(self):
        plot_frame = ttk.Frame(self.root)
        plot_frame.pack(side=tk.TOP, fill=tk.BOTH, expand=True)

        self.fig = Figure(figsize=(10, 6), dpi=100)
        self.ax = self.fig.add_subplot(111)
        self.canvas = FigureCanvasTkAgg(self.fig, master=plot_frame)
        self.canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)

    # --------------------------------------------------------
    # Event Handlers
    # --------------------------------------------------------
    def on_category_change(self):
        cat = self.category_var.get()
        if not cat:
            self.identifier_cb["values"] = []
            self.identifier_var.set("")
            return

        subset = self.df[self.df["category_type"] == cat]
        identifiers = sorted(subset["identifier"].unique().tolist())

        self.identifier_cb["values"] = identifiers
        if identifiers:
            self.identifier_var.set(identifiers[0])

    def on_plot(self):
        cat = self.category_var.get()
        ident = self.identifier_var.get()

        if not cat or not ident:
            messagebox.showinfo("Missing Fields", "Select category and identifier.")
            return

        subset = self.df[
            (self.df["category_type"] == cat) &
            (self.df["identifier"] == ident)
        ]

        if subset.empty:
            messagebox.showinfo("No data", "No records found.")
            return

        # Prep sorting
        def prep(df_local):
            df_local = df_local.copy()
            df_local["date_obj"] = pd.to_datetime(
                "2024-" + df_local["MM_DD"], format="%Y-%m-%d", errors="coerce"
            )
            return df_local.sort_values("date_obj")

        # Subsets grouped by stock/metric
        stock_series = {}
        for code in STOCK_CONFIGS.keys():
            stock_series[code] = {
                "ten_year": prep(subset[(subset["stock"] == code) & (subset["metric_type"] == "ten_year_avg")]),
                "current": prep(subset[(subset["stock"] == code) & (subset["metric_type"] == "current_year")]),
            }

        # Clear plot
        self.ax.clear()

        # Plot each stock with consistent styling
        active_names = []
        for code, cfg in STOCK_CONFIGS.items():
            series = stock_series.get(code, {})
            ten_year = series.get("ten_year")
            current = series.get("current")

            if ten_year is not None and not ten_year.empty:
                self.ax.plot(
                    ten_year["date_obj"],
                    ten_year["value"],
                    color=cfg["color"],
                    alpha=0.35,
                    linewidth=2,
                    label=f"{cfg['name']} — 10yr",
                )
                active_names.append(cfg["name"])

            if current is not None and not current.empty:
                self.ax.plot(
                    current["date_obj"],
                    current["value"],
                    color=cfg["color"],
                    linewidth=2.5,
                    label=f"{cfg['name']} — {CURRENT_YEAR}",
                )
                if cfg["name"] not in active_names:
                    active_names.append(cfg["name"])

        # Labels / formatting
        title_suffix = ", ".join(active_names) if active_names else ", ".join(cfg["name"] for cfg in STOCK_CONFIGS.values())
        self.ax.set_title(f"{ident} ({cat}) — {title_suffix}", fontsize=14, pad=12)
        self.ax.set_xlabel("Date (MM-DD)")
        self.ax.set_ylabel("Fish per Week")
        self.ax.grid(alpha=0.3)
        self.ax.legend()

        self.fig.autofmt_xdate(rotation=45)
        self.canvas.draw()


# ------------------------------------------------------------
# Entrypoint
# ------------------------------------------------------------
if __name__ == "__main__":
    root = tk.Tk()
    app = RunReportApp(root)
    root.mainloop()
