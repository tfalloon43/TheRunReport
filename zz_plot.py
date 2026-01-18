#!/usr/bin/env python3
import sqlite3
import tkinter as tk
from pathlib import Path
from tkinter import ttk

import pandas as pd
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.figure import Figure

DEFAULT_DB = Path(__file__).resolve().parent / "runreport-backend" / "0_db" / "local.db"
DEFAULT_TABLE = "EscapementReport_PlotData"


def fetch_rivers(conn: sqlite3.Connection) -> list[str]:
    query = f"SELECT DISTINCT river FROM {DEFAULT_TABLE} WHERE river IS NOT NULL ORDER BY river ASC;"
    df = pd.read_sql_query(query, conn)
    return df["river"].dropna().astype(str).str.strip().tolist()


def fetch_species(conn: sqlite3.Connection, river: str) -> list[str]:
    query = (
        f"SELECT DISTINCT Species_Plot FROM {DEFAULT_TABLE} "
        "WHERE river = ? AND Species_Plot IS NOT NULL ORDER BY Species_Plot ASC;"
    )
    df = pd.read_sql_query(query, conn, params=(river,))
    return df["Species_Plot"].dropna().astype(str).str.strip().tolist()


def fetch_series(conn: sqlite3.Connection, river: str, species: str) -> pd.DataFrame:
    query = (
        f'SELECT "MM-DD", current_year, previous_year, "10_year" '
        f"FROM {DEFAULT_TABLE} "
        "WHERE river = ? AND Species_Plot = ? "
        'ORDER BY "MM-DD" ASC;'
    )
    df = pd.read_sql_query(query, conn, params=(river, species))
    if df.empty:
        return df
    df["date_obj"] = pd.to_datetime("2024-" + df["MM-DD"], errors="coerce")
    return df.dropna(subset=["date_obj"]).sort_values("date_obj")


class PlotApp:
    def __init__(self, root: tk.Tk, db_path: Path):
        self.root = root
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)

        self.root.title("Escapement Plot Viewer")
        self.root.geometry("960x600")

        self.river_var = tk.StringVar()
        self.species_var = tk.StringVar()

        control_frame = ttk.Frame(root, padding=12)
        control_frame.pack(side=tk.TOP, fill=tk.X)

        ttk.Label(control_frame, text="River").pack(side=tk.LEFT, padx=(0, 8))
        self.river_combo = ttk.Combobox(control_frame, textvariable=self.river_var, state="readonly", width=30)
        self.river_combo.pack(side=tk.LEFT, padx=(0, 16))
        self.river_combo.bind("<<ComboboxSelected>>", self.on_river_change)

        ttk.Label(control_frame, text="Species").pack(side=tk.LEFT, padx=(0, 8))
        self.species_combo = ttk.Combobox(control_frame, textvariable=self.species_var, state="readonly", width=30)
        self.species_combo.pack(side=tk.LEFT, padx=(0, 16))
        self.species_combo.bind("<<ComboboxSelected>>", self.on_species_change)

        self.status_label = ttk.Label(control_frame, text="")
        self.status_label.pack(side=tk.RIGHT)

        self.fig = Figure(figsize=(9, 4.6), dpi=100)
        self.ax = self.fig.add_subplot(111)
        self.canvas = FigureCanvasTkAgg(self.fig, master=root)
        self.canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)

        self.load_rivers()

    def load_rivers(self) -> None:
        rivers = fetch_rivers(self.conn)
        self.river_combo["values"] = rivers
        if rivers:
            self.river_var.set(rivers[0])
            self.load_species(rivers[0])
        else:
            self.status_label.config(text="No rivers found.")

    def load_species(self, river: str) -> None:
        species = fetch_species(self.conn, river)
        self.species_combo["values"] = species
        if species:
            self.species_var.set(species[0])
            self.update_plot()
        else:
            self.species_var.set("")
            self.update_plot(empty=True)

    def on_river_change(self, _event=None) -> None:
        river = self.river_var.get().strip()
        if river:
            self.load_species(river)

    def on_species_change(self, _event=None) -> None:
        self.update_plot()

    def update_plot(self, empty: bool = False) -> None:
        self.ax.clear()
        if empty:
            self.status_label.config(text="No species for selected river.")
            self.canvas.draw()
            return

        river = self.river_var.get().strip()
        species = self.species_var.get().strip()
        if not river or not species:
            self.status_label.config(text="Select river and species.")
            self.canvas.draw()
            return

        df = fetch_series(self.conn, river, species)
        if df.empty:
            self.status_label.config(text="No data for selection.")
            self.canvas.draw()
            return

        self.ax.plot(df["date_obj"], df["current_year"], label="Current Year", linewidth=2)
        self.ax.plot(df["date_obj"], df["previous_year"], label="Previous Year", linewidth=2)
        self.ax.plot(df["date_obj"], df["10_year"], label="10-Year Avg", linewidth=2)
        self.ax.set_title(f"{river} - {species}")
        self.ax.set_xlabel("Date")
        self.ax.set_ylabel("Fish count")
        self.ax.grid(True, alpha=0.3)
        self.ax.legend()
        self.status_label.config(text=f"{len(df):,} points")
        self.fig.autofmt_xdate()
        self.canvas.draw()


def main() -> None:
    db_path = DEFAULT_DB
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")
    root = tk.Tk()
    PlotApp(root, db_path)
    root.mainloop()


if __name__ == "__main__":
    main()
