# view_data.py
import sqlite3
import pandas as pd

def export_escapement_to_csv(
    db_path="pdf_data.sqlite",
    #table="escapement_reordered",
    #out_file="escapement_reordered.csv"
    table="escapement_reduced",
    out_file="escapement_reduced.csv"
):
    # Connect to SQLite
    conn = sqlite3.connect(db_path)

    # Read the whole table
    df = pd.read_sql(f"SELECT * FROM {table}", conn)

    # Save to CSV
    df.to_csv(out_file, index=False)

    conn.close()
    print(f"✅ Exported {len(df)} rows from {table} to {out_file}")

if __name__ == "__main__":
    export_escapement_to_csv()
    
    ## test for github