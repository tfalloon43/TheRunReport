# create_datatable.py (with hatch_name + parsed date columns)
import sqlite3
import re
import pandas as pd

db_path = "pdf_data.sqlite"
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# --- (re)create escapement_data table ---
cursor.execute("DROP TABLE IF EXISTS escapement_data")
conn.commit()

cursor.execute("""
CREATE TABLE IF NOT EXISTS escapement_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pdf_name TEXT,
    page_num INTEGER,
    species TEXT,
    hatchery TEXT,
    hatch_name TEXT,
    stock TEXT,
    adult_total TEXT,
    date TEXT,            -- original string from PDF (kept for traceability)
    date_iso TEXT,        -- normalized YYYY-MM-DD for sorting/joins
    year INTEGER,
    month INTEGER,
    day INTEGER,
    family TEXT,
    basin TEXT
)
""")
conn.commit()

# --- load source lines ---
cursor.execute("SELECT pdf_name, page_num, text_line FROM pdf_lines ORDER BY id")
lines = cursor.fetchall()

# --- patterns + helpers ---
date_pattern = re.compile(r"\d{2}/\d{2}/\d{2}")        # MM/DD/YY
number_pattern = re.compile(r"[\d,]+")                 # numbers (with commas)

species_headers = [
    "Fall Chinook", "Spring Chinook", "Summer Chinook",
    "Coho", "Summer Steelhead", "Sockeye", "Late Coho", "Summer Coho",
    "Type N Coho", "Type S Coho", "Chum", "Odd Year Pink", "Summer Chum", "Pink",
    "Summer Pink", "Winter Steelhead", "Winter-Late Steelhead", "Fall Chum",
    "Anadromous Coastal Cutthroat", "Lahontan Cutthroat", "Westslope Cutthroat",
    "Dolly/Bull Trout", "Whitefish", "Late Kokanee", "Kokanee", "Sucker: General",
    "Northern Pikeminnow"
]
family_map = {
    "fall chinook": "chinook", "spring chinook": "chinook", "summer chinook": "chinook",
    "late coho": "coho", "summer coho": "coho", "type n coho": "coho", "type s coho": "coho",
    "coho": "coho",
    "summer steelhead": "steelhead", "winter steelhead": "steelhead", "winter-late steelhead": "steelhead",
    "steelhead": "steelhead",
    "sockeye": "sockeye", "chum": "chum", "summer chum": "chum",
    "pink": "pink", "summer pink": "pink",
    "dolly/bull trout": "bull trout", "whitefish": "whitefish",
    "sucker: general": "sucker", "kokanee": "kokanee", "late kokanee": "kokanee",
    "northern pikeminnow": "pikeminnow",
    "anadromous coastal cutthroat": "cutthroat", "lahontan cutthroat": "cutthroat", "westslope cutthroat": "cutthroat"
}

# hatch_name_map
hatch_name_map = {
    "GEORGE ADAMS HATCHERY": "George Adams Hatchery",
    "GEORGE ADAMS HATCHRY": "George Adams Hatchery",
    "HOODSPORT HATCHERY": "Hoodsport Hatchery",
    "SOOS CREEK HATCHERY": "Soos Creek Hatchery",
    "ICY CR HATCHERY": "Icy Creek Hatchery",
    "PALMER HATCHERY": "Palmer Hatchery",
    "GARRISON HATCHERY": "Garrison Hatchery",
    "MINTER CR HATCHERY": "Minter Creek Hatchery",
    "NASELLE HATCHERY": "Naselle Hatchery",
    "FOSTER RD TRAP": "Foster Road Trap",
    "BEAVER CR HATCHERY": "Beaver Creek Hatchery",
    "ELOCHOMAN SILL TRAP": "Elochoman Sill Trap",
    "COWLITZ SALMON HATCHERY": "Cowlitz Salmon Hatchery",
    "COWLITZ SALMON HATCH": "Cowlitz Salmon Hatchery",
    "COWLITZ TROUT HATCH": "Cowlitz Trout Hatchery",
    "COWLITZ SALMON": "Cowlitz Salmon Hatchery",
    "KALAMA FALLS HATCHRY": "Kalama Falls Hatchery",
    "KALAMA FALLS HATCHERY": "Kalama Falls Hatchery",
    "FALLERT CR HATCHERY": "Fallery Creek Hatchery",
    "MODROW TRAP": "Modrow Trap",
    "LEWIS RIVER HATCHERY": "Lewis River Hatchery",
    "MERWIN DAM FCF": "Merwin Dam FCF",
    "MERWIN HATCHERY": "Merwin Hatchery",
    "SPEELYAI HATCHERY": "Speelyai Hatchery",
    "LYONS FERRY HATCHERY": "Lyons Ferry Hatchery",
    "MARBLEMOUNT HATCHERY": "Marblemount Hatchery",
    "HURD CR HATCHERY": "Hurd Creek Hatchery",
    "DUNGENESS HATCHERY": "Dungeness Hatchery",
    "DAYTON ACCLIMA. POND": "Dayton Acclimation Pond",
    "TUCANNON HATCHERY": "Tucannon Hatchery",
    "EASTBANK HATCHERY": "Eastbank Hatchery",
    "CHIWAWA HATCHERY": "Chiwawa Hatchery",
    "SUNSET FALLS FCF": "Sunset Falls FCF",
    "WALLACE R HATCHERY": "Wallace River Hatchery",
    "SOLDUC HATCHERY": "Sol Duc Hatchery",
    "KENDALL CR HATCHERY": "Kendall Creek Hatchery",
    "BAKER LK HATCHERY": "Baker Lake Hatchery",
    "CEDAR RIVER HATCHERY": "Cedar River Hatchery",
    "REITER PONDS": "Reiter Ponds",
    "BOGACHIEL HATCHERY": "Bogachiel Hatchery",
    "WASHOUGAL HATCHERY": "Washougal Hatchery",
    "WASHOUGAL RIVER FISH": "Washougal River Weir",
    "WASHOUGAL RIVER FISH WEIR": "Washougal River Weir",
    "SKAMANIA HATCHERY": "Skamania Hatchery",
    "WHATCOM CR HATCHERY": "Whatcom Creek Hatchery",
    "SAMISH HATCHERY": "Samish Hatchery",
    "TOKUL CR HATCHERY": "Tokul Creek Hatchery",
    "ISSAQUAH HATCHERY": "Issaquah Hatchery",
    "VOIGHTS CR HATCHERY": "Voights Creek Hatchery",
    "TUMWATER FALLS HATCH": "Tumwater Falls Hatchery",
    "TUMWATER FALLS HATCHERY": "Tumwater Falls Hatchery",
    "TUMWATER FALLS": "Tumwater Falls Hatchery",
    "GLENWOOD SPRINGS": "Glenwood Springs Hatchery",
    "MORSE CREEK HATCHERY": "Morse Creek Hatchery",
    "ELWHA HATCHERY": "Elwha Hatchery",
    "HUMPTULIPS HATCHERY": "Humptulips Hatchery",
    "MAYR BROTHERS REARIN": "Mayr Brothers Rearing",
    "LK ABERDEEN HATCHERY": "Lake Aberdeen Hatchery",
    "WYNOOCHEE R DAM TRAP": "Wynoochee River Dam Trap",
    "TACOMA POWER": "Tacoma Power – Wynoochee River",
    "BINGHAM CR HATCHERY": "Bingham Creek Hatchery",
    "SATSOP SPRINGS PONDS": "Satsop Springs Ponds",
    "FORKS CREEK HATCHERY": "Forks Creek Hatchery",
    "NEMAH HATCHERY": "Nemah Hatchery",
    "GRAYS RIVER HATCHERY": "Grays River Hatchery",
    "NORTH TOUTLE HATCHRY": "Toutle River Hatchery",
    "NORTH TOUTLE HATCHERY": "Toutle River Hatchery",
    "PRIEST RAPIDS HATCHERY": "Priest Rapids Hatchery",
    "RINGOLD SPRINGS HATCHERY": "Ringold Springs Hatchery",
    "RINGOLD SPRINGS": "Ringold Springs Hatchery",
    "METHOW HATCHERY": "Methow Hatchery",
    "WHITEHORSE POND": "Whitehorse Pond",
    "WELLS HATCHERY": "Wells Hatchery",
    "SKOOKUMCHUCK HATCHERY": "Skookumchuck Hatchery",
    "SKOOKUMCHUCK HATCHRY": "Skookumchuck Hatchery",
    "MCKERNAN HATCHERY": "McKernan Hatchery",
    "CHELAN HATCHERY": "Chelan Hatchery",
    "CHELAN PUD HATCHERY": "Chelan Hatchery",
    "SPOKANE HATCHERY": "Spokane Hatchery",
    "LK WHATCOM HATCHERY": "Lake Whatcom Hatchery",
    "OMAK HATCHERY": "Omak Hatchery",
    "TWISP ACCLIMATION PD": "Twisp Acclimation Pond",
    "COTTONWOOD CR POND":"Cottonwood Creek Pond",
    "GRAYS RIVER WEIR":"Grays River Weir",
}

# basin_map (unchanged from your version)
basin_map = {
    "GEORGE ADAMS HATCHERY": "Skokomish River",
    "GEORGE ADAMS HATCHRY": "Skokomish River",
    "HOODSPORT HATCHERY": "Hood Canal",
    "SOOS CREEK HATCHERY": "Green River",
    "ICY CR HATCHERY": "Green River",
    "PALMER HATCHERY": "Green River",
    "GARRISON HATCHERY": "Chambers Creek",
    "MINTER CR HATCHERY": "Minter Creek",
    "NASELLE HATCHERY": "Naselle River",
    "FOSTER RD TRAP": "Elochoman River",
    "BEAVER CR HATCHERY": "Elochoman River",
    "ELOCHOMAN SILL TRAP": "Elochoman River",
    "COWLITZ SALMON HATCHERY": "Cowlitz River",
    "COWLITZ SALMON HATCH": "Cowlitz River",
    "COWLITZ TROUT HATCH": "Cowlitz River",
    "COWLITZ SALMON": "Cowlitz River",
    "KALAMA FALLS HATCHRY": "Kalama River",
    "KALAMA FALLS HATCHERY": "Kalama River",
    "FALLERT CR HATCHERY": "Kalama River",
    "MODROW TRAP": "Kalama River",
    "LEWIS RIVER HATCHERY": "Lewis River",
    "MERWIN DAM FCF": "Lewis River",
    "MERWIN HATCHERY": "Lewis River",
    "SPEELYAI HATCHERY": "Lewis River",
    "LYONS FERRY HATCHERY": "Snake River",
    "MARBLEMOUNT HATCHERY": "Skagit River",
    "HURD CR HATCHERY": "Dungeness River",
    "DUNGENESS HATCHERY": "Dungeness River",
    "DAYTON ACCLIMA. POND": "Touchet River",
    "TUCANNON HATCHERY": "Tucannon River",
    "EASTBANK HATCHERY": "Columbia River - Rocky Reach Dam",
    "CHIWAWA HATCHERY": "Chiwawa River", #<-- cool spots
    "SUNSET FALLS FCF": "Skykomish River",
    "WALLACE R HATCHERY": "Wallace River",
    "SOLDUC HATCHERY": "Sol Duc River",
    "KENDALL CR HATCHERY": "North Fork Nooksack River",
    "BAKER LK HATCHERY": "Baker Lake/Baker River",
    "CEDAR RIVER HATCHERY": "Cedar River",
    "REITER PONDS": "Skykomish River",
    "BOGACHIEL HATCHERY": "Bogachiel River",
    "WASHOUGAL HATCHERY": "Washougal River",
    "WASHOUGAL RIVER FISH": "Washougal River",
    "WASHOUGAL RIVER FISH WEIR": "Washougal River",
    "SKAMANIA HATCHERY": "Washougal River",
    "WHATCOM CR HATCHERY": "Whatcom Creek",
    "SAMISH HATCHERY": "Samish River",
    "TOKUL CR HATCHERY": "Snoqualmie River",
    "ISSAQUAH HATCHERY": "Issaquah Creek",
    "VOIGHTS CR HATCHERY": "Puyallup River",
    "TUMWATER FALLS HATCH": "Deschutes River",
    "TUMWATER FALLS HATCHERY": "Deschutes River",
    "TUMWATER FALLS": "Deschutes River",
    "GLENWOOD SPRINGS": "Nooksack River",
    "MORSE CREEK HATCHERY": "Elwha River",
    "ELWHA HATCHERY": "Elwha River",
    "HUMPTULIPS HATCHERY": " Humptulips River",
    "MAYR BROTHERS REARIN": "Wishkah River",
    "LK ABERDEEN HATCHERY": "Chehalis River",
    "WYNOOCHEE R DAM TRAP": "Wynoochee River",
    "TACOMA POWER": "Wynoochee River",
    "BINGHAM CR HATCHERY": "Satsop River",
    "SATSOP SPRINGS PONDS": "Satsop River",
    "FORKS CREEK HATCHERY": "Willapa River",
    "NEMAH HATCHERY": "Nemah River",
    "GRAYS RIVER HATCHERY": "Grays River",
    "NORTH TOUTLE HATCHRY": "Toutle River",
    "NORTH TOUTLE HATCHERY": "Toutle River",
    "NORTH TOUTLE HATCHERY": "Toutle River",
    "PRIEST RAPIDS HATCHERY": "Columbia River - Priest Rapids Dam",
    "RINGOLD SPRINGS HATCHERY": "Columbia River - Priest Rapids Dam",
    "RINGOLD SPRINGS": "Columbia River - Priest Rapids Dam",
    "METHOW HATCHERY": "Methow River",
    "WHITEHORSE POND": "Stillaguamish River",
    "WELLS HATCHERY": "Columbia River - Wells Dam",
    "SKOOKUMCHUCK HATCHERY": "Skookumchuck River",
    "SKOOKUMCHUCK HATCHRY": "Skookumchuck River",
    "MCKERNAN HATCHERY": "Skokomish River",
    "CHELAN HATCHERY": "Chelan River",
    "CHELAN PUD HATCHERY": "Chelan River",
    "SPOKANE HATCHERY": "Spokane River",
    "LK WHATCOM HATCHERY": "Whatcom Lake",
    "OMAK HATCHERY": "Okanogan River",
    "TWISP ACCLIMATION PD": "Twisp River",
}

species_set_lc = {s.lower() for s in species_headers}

def standalone_H(text: str) -> bool:
    return bool(re.search(r'\bH\b', text, re.IGNORECASE))

def standalone_W(text: str) -> bool:
    return bool(re.search(r'\bW\b', text, re.IGNORECASE))

def stock_from_text(text: str):
    """Return 'H', 'W', or 'U' if clearly present in text (Stock- H/W/U or standalone H/W/U), else None."""
    if text is None:
        return None
    t = text.strip()
    if re.search(r'\bStock-?\s*H\b', t, re.IGNORECASE):
        return 'H'
    if re.search(r'\bStock-?\s*W\b', t, re.IGNORECASE):
        return 'W'
    if re.search(r'\bStock-?\s*U\b', t, re.IGNORECASE):
        return 'U'
    if re.search(r'\bH\b', t):
        return 'H'
    if re.search(r'\bW\b', t):
        return 'W'
    if re.search(r'\bU\b', t):
        return 'U'
    return None

def strip_trailing_stock_token(hatch_text: str) -> str:
    if not hatch_text:
        return hatch_text
    cleaned = re.sub(r'(?:\s|^|[-(])(?:Stock-?\s*[HWU]|[HWU])\s*$', '', hatch_text, flags=re.IGNORECASE).strip()
    cleaned = re.sub(r'\s{2,}', ' ', cleaned)
    return cleaned

current_species = None
i = 0
while i < len(lines):
    pdf_name, page_num, raw_line = lines[i]
    line = (raw_line or "").strip()
    if not line:
        i += 1
        continue

    # species header detection
    if line.lower() in species_set_lc:
        for sp in species_headers:
            if sp.lower() == line.lower():
                current_species = sp
                break
        i += 1
        continue

    # date (raw) from line
    date_matches = date_pattern.findall(line)
    date_val = date_matches[-1] if date_matches else None

    # normalize/parse the date early
    if date_val:
        # first try MM/DD/YY
        dt = pd.to_datetime(date_val, format="%m/%d/%y", errors="coerce")
        if pd.isna(dt):
            # fallback: MM/DD/YYYY
            dt = pd.to_datetime(date_val, format="%m/%d/%Y", errors="coerce")
    else:
        dt = pd.NaT

    date_iso = dt.strftime("%Y-%m-%d") if pd.notna(dt) else None
    year = int(dt.year) if pd.notna(dt) else None
    month = int(dt.month) if pd.notna(dt) else None
    day = int(dt.day) if pd.notna(dt) else None

    # clean text
    line_clean = re.sub(r'\bI-205\b', '', line, flags=re.IGNORECASE)
    line_clean = re.sub(r'\s{2,}', ' ', line_clean).strip()

    # numbers
    numbers = number_pattern.findall(line_clean)
    adult_total = numbers[0] if numbers else None

    # hatchery text
    if numbers:
        num_index = line_clean.find(numbers[0])
        hatchery_text = line_clean[:num_index].strip()
    else:
        hatchery_text = line_clean

    # stock detection
    stock_val = None
    m_tail = re.search(r'(?:^|[\s\-\(])([HWU])\s*$', hatchery_text, re.IGNORECASE)
    if m_tail:
        tail_tok = m_tail.group(1).upper()
        if tail_tok in ('H', 'W', 'U'):
            stock_val = tail_tok
        hatchery_text = strip_trailing_stock_token(hatchery_text)

    if stock_val is None and numbers:
        pre_num = line_clean[:num_index].rstrip()
        stock_val = stock_from_text(pre_num)
        if stock_val:
            hatchery_text = re.sub(r'(?:\bStock-?\s*[HWU]\b|\b[HWU]\b)\s*$', '', pre_num, flags=re.IGNORECASE).strip()

    if stock_val is None:
        next_text = lines[i + 1][2].strip() if (i + 1) < len(lines) else ''
        if next_text.strip().lower() == "river- u":
            stock_val = "U"
        else:
            stock_val = stock_from_text(next_text)

    if stock_val is None:
        stock_val = 'IDK'

    # family
    family_val = family_map.get(current_species.lower(), None) if current_species else None

    # basin
    basin_val = None
    hatchery_upper = (hatchery_text or "").upper()
    for key, basin in basin_map.items():
        if key in hatchery_upper:
            basin_val = basin
            break

    # hatch_name
    hatch_name_val = None
    for key, nice_name in hatch_name_map.items():
        if key in hatchery_upper:
            hatch_name_val = nice_name
            break
    if hatch_name_val is None:
        # fallback: title-cased hatchery_text
        hatch_name_val = hatchery_text.strip().title() if hatchery_text else None

    # insert row
    if hatchery_text or adult_total or stock_val or date_val:
        cursor.execute("""
            INSERT INTO escapement_data
            (pdf_name, page_num, species, hatchery, hatch_name, stock, adult_total, date, date_iso, year, month, day, family, basin)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            pdf_name, page_num, current_species,
            hatchery_text, hatch_name_val, stock_val,
            adult_total, date_val, date_iso, year, month, day,
            family_val, basin_val
        ))

    i += 1

conn.commit()

# --- POST-PROCESSING: merge stock-only rows up into the previous row ---
df = pd.read_sql_query("SELECT * FROM escapement_data ORDER BY id", conn)

rows_to_delete = []
for idx in range(len(df)):
    row = df.loc[idx]
    hatch = (row['hatchery'] or "").strip()
    if hatch == "" or re.match(r'(?i)^\s*stock-?', hatch):
        stock_here = row['stock']
        if pd.notna(stock_here) and str(stock_here).strip() != "":
            prev_idx = idx - 1
            if prev_idx >= 0:
                prev_row = df.loc[prev_idx]
                if prev_row['pdf_name'] == row['pdf_name'] and prev_row['species'] == row['species']:
                    prev_stock = prev_row['stock']
                    if pd.isna(prev_stock) or str(prev_stock).strip() in ('', 'IDK', 'None', 'NONE'):
                        cursor.execute("UPDATE escapement_data SET stock = ? WHERE id = ?", (stock_here, int(prev_row['id'])))
                        rows_to_delete.append(int(row['id']))

for rid in rows_to_delete:
    cursor.execute("DELETE FROM escapement_data WHERE id = ?", (rid,))

conn.commit()
conn.close()
print("escapement_data table created with hatch_name + parsed dates (date_iso/year/month/day), basin + stock handling applied.")