# 1_app.py — updated to plot all H & W lines on same chart
# ------------------------------------------------------------
# Lines shown:
#   H 10-year avg     → faint blue
#   H current-year    → solid blue
#   W 10-year avg     → faint red
#   W current-year    → solid red
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

        # Subsets
        H_10 = prep(subset[(subset["stock"] == "H") & (subset["metric_type"] == "ten_year_avg")])
        H_cur = prep(subset[(subset["stock"] == "H") & (subset["metric_type"] == "current_year")])
        W_10 = prep(subset[(subset["stock"] == "W") & (subset["metric_type"] == "ten_year_avg")])
        W_cur = prep(subset[(subset["stock"] == "W") & (subset["metric_type"] == "current_year")])

        # Clear plot
        self.ax.clear()

        # Plot H (blue)
        if not H_10.empty:
            self.ax.plot(H_10["date_obj"], H_10["value"], color="blue", alpha=0.35, linewidth=2, label="Hatchery — 10yr")
        if not H_cur.empty:
            self.ax.plot(H_cur["date_obj"], H_cur["value"], color="blue", linewidth=2.5, label="Hatchery — 2025")

        # Plot W (red)
        if not W_10.empty:
            self.ax.plot(W_10["date_obj"], W_10["value"], color="red", alpha=0.35, linewidth=2, label="Wild — 10yr")
        if not W_cur.empty:
            self.ax.plot(W_cur["date_obj"], W_cur["value"], color="red", linewidth=2.5, label="Wild — 2025")

        # Labels / formatting
        self.ax.set_title(f"{ident} ({cat}) — Hatchery & Wild", fontsize=14, pad=12)
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