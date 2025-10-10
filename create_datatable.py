# create_datatable.py — v7
# ------------------------------------------------------------
# Cleans and merges raw pdf_lines data into structured format
# Adds: species + family + hatchery + hatch_name + basin + stock_BO + stock + date
# Reference data (species_headers, family_map, hatch_name_map, basin_map, stock_corrections)
# is imported from lookup_maps.py
# ------------------------------------------------------------

import sqlite3
import pandas as pd
import re
from lookup_maps import species_headers, family_map, hatch_name_map, basin_map, stock_corrections

# ------------------------------------------------------------
# 1) Connect and load pdf_lines
# ------------------------------------------------------------
db_path = "pdf_data.sqlite"
conn = sqlite3.connect(db_path)
cursor = conn.cursor()
cursor.execute("SELECT pdf_name, page_num, text_line FROM pdf_lines ORDER BY id")
rows = cursor.fetchall()

# ------------------------------------------------------------
# 2) Setup
# ------------------------------------------------------------
species_set_lc = {s.lower() for s in species_headers}

# ------------------------------------------------------------
# 3) Helper functions
# ------------------------------------------------------------
def is_species_header(text: str) -> bool:
    """Check if a line exactly matches a species header."""
    return text.strip().lower() in species_set_lc

def contains_stock_tag(text: str) -> bool:
    """Check if a short line contains - W, - H, - U, or - M"""
    return bool(re.search(r"-\s*[WHUMwhum]\b", text))

def find_insert_pos(upper_text: str) -> int:
    """Find where to insert continuation lines before first number or dash."""
    match = re.search(r"(\d| -)", upper_text)
    return match.start() if match else len(upper_text)

def extract_hatchery(text):
    """Extract the all-uppercase hatchery name prefix."""
    if not text:
        return None
    text = text.strip()
    match = re.match(r"^([A-Z0-9 ./-]+)(?=\s[A-Z][a-z])", text)
    if match:
        return match.group(1).strip()
    parts = text.split()
    upper_parts = [w for w in parts if w.isupper()]
    if len(upper_parts) >= 2:
        return " ".join(upper_parts[:2])
    elif upper_parts:
        return upper_parts[0]
    return None

def lookup_hatch_name(hatch):
    if not hatch:
        return None
    return hatch_name_map.get(hatch.upper(), hatch.title())

def lookup_basin(hatch):
    if not hatch:
        return None
    return basin_map.get(hatch.upper())

# ------------------------------------------------------------
# 4) Merge continuation lines + repair inline Stock- tags
# ------------------------------------------------------------
fixed = [list(r) for r in rows]

def move_trailing_stock_letter(s: str) -> str:
    """
    If a line contains 'Stock-' followed by a number/dash but no letter after it,
    and there is a standalone H/W/U/M token (often at the end), move that letter
    right after 'Stock- '. Also tidy ' H Hatchery' residue, and tolerate
    trailing whitespace/punctuation.
    """
    if "Stock-" not in s:
        return s

    # Only act if 'Stock-' is followed somewhere by a number/dash (the counts section)
    if not re.search(r"Stock-\s*[\d-]", s):
        return s

    # If there's already a letter after Stock-, nothing to do
    if re.search(r"Stock-\s*[HWUM]\b", s):
        return s

    # Prefer a trailing standalone token first (e.g., "... estimate. H")
    tail = re.search(r'\s([HWUM])\s*\.?\s*$', s)
    if tail:
        letter = tail.group(1)
        # Remove that trailing letter chunk
        s = s[:tail.start(1)].rstrip()
    else:
        # Otherwise, take the last standalone letter anywhere in the line
        last = None
        for m in re.finditer(r'(?<![A-Za-z0-9])([HWUM])(?![A-Za-z0-9])', s):
            last = m
        if not last:
            return s
        letter = last.group(1)
        s = (s[:last.start()] + s[last.end():]).strip()

    # Tidy cases like " H Hatchery" / " H. Hatchery"
    s = re.sub(r'\s+[HWUM]\s+(Hatchery\.?|HATCHERY\.?)', r' \1', s)

    # Inject the letter after 'Stock- '
    s = re.sub(r'(Stock-\s*)', rf'\1{letter} ', s, count=1)

    # Collapse spaces
    s = re.sub(r'\s{2,}', ' ', s).strip()
    return s

for i in range(1, len(fixed)):
    prev_txt = (fixed[i-1][2] or "").strip()
    cur_txt  = (fixed[i][2]  or "").strip()

    if not cur_txt:
        continue
    if is_species_header(cur_txt):
        continue

    # === Case 1 & 2: short-line continuations (<= 30 chars) ===
    if len(cur_txt) <= 30:
        if contains_stock_tag(cur_txt):  # Case 1: insert before first number/dash
            insert_at = find_insert_pos(prev_txt)
            if insert_at > 0:
                fixed[i-1][2] = (
                    prev_txt[:insert_at].rstrip() + " " + cur_txt + " " + prev_txt[insert_at:]
                ).strip()
                fixed[i][2] = ""
            continue
        else:  # Case 2: append to the end
            fixed[i-1][2] = (prev_txt + " " + cur_txt).strip()
            fixed[i][2] = ""
            continue

    # === Case 3: Stock- has numbers/dashes after it, but letter appears elsewhere ===
    if "Stock-" in cur_txt and re.search(r"Stock-\s*[\d-]", cur_txt):
        if not re.search(r"Stock-\s*[HWUM]\b", cur_txt):
            new_txt = move_trailing_stock_letter(cur_txt)
            fixed[i][2] = new_txt
        continue

    # === Case 4: This line is a long 'Stock- H/W/U/M ...' continuation
    if re.match(r'^\s*Stock-\s*[HWUM]\b', cur_txt):
        combined = (prev_txt + " " + cur_txt).strip()
        combined = move_trailing_stock_letter(combined)
        fixed[i-1][2] = combined
        fixed[i][2] = ""
        continue

    # === Case 5: Ends with standalone H/W/U/M (possibly with a dot) ===
    if "Stock-" in cur_txt and re.search(r"Stock-\s*[\d-]", cur_txt) and re.search(r'\s([HWUM])\s*\.?\s*$', cur_txt):
        fixed[i][2] = move_trailing_stock_letter(cur_txt)
        continue

# --- Final normalization pass: repair any missed lines globally ---
for j in range(len(fixed)):
    txt = (fixed[j][2] or "").strip()
    if txt:
        fixed[j][2] = move_trailing_stock_letter(txt)
        
# ------------------------------------------------------------
# 5) Save merged lines
# ------------------------------------------------------------
merged_df = pd.DataFrame(fixed, columns=["pdf_name", "page_num", "text_line"])
merged_df.to_csv("pdf_lines_fixed.csv", index=False)
print("✅ Saved merged lines → pdf_lines_fixed.csv")

# ------------------------------------------------------------
# 6) Add species + family tagging
# ------------------------------------------------------------
labeled_rows = []
current_species = None

for pdf_name, page_num, text_line in merged_df.itertuples(index=False):
    line = (text_line or "").strip()
    if not line:
        labeled_rows.append([pdf_name, page_num, text_line, None, None])
        continue
    if is_species_header(line):
        current_species = line.strip()
        labeled_rows.append([
            pdf_name, page_num, line, current_species,
            family_map.get(current_species.lower())
        ])
        continue
    species_val = current_species
    family_val = family_map.get(species_val.lower()) if species_val else None
    labeled_rows.append([pdf_name, page_num, text_line, species_val, family_val])

labeled_df = pd.DataFrame(
    labeled_rows, columns=["pdf_name", "page_num", "text_line", "species", "family"]
)

# ------------------------------------------------------------
# 7) Add hatchery + hatch_name + basin
# ------------------------------------------------------------
labeled_df["Hatchery"] = labeled_df["text_line"].apply(extract_hatchery)
labeled_df["hatch_name"] = labeled_df["Hatchery"].apply(lookup_hatch_name)
labeled_df["basin"] = labeled_df["Hatchery"].apply(lookup_basin)

# ------------------------------------------------------------
# 8) Extract stock_BO (between hatchery and stock tag)
# ------------------------------------------------------------
def extract_stock_bo(text, hatchery):
    if not text or not hatchery:
        return None
    remainder = text[len(hatchery):].strip()
    match = re.search(r"([A-Za-z0-9().\s]+?-\s*[HWMU])\b", remainder)
    return match.group(1).strip() if match else None

labeled_df["stock_BO"] = labeled_df.apply(
    lambda row: extract_stock_bo(row["text_line"], row["Hatchery"]), axis=1
)

# ------------------------------------------------------------
# 9) Apply manual stock corrections
# ------------------------------------------------------------
def apply_stock_corrections(stock_value):
    if not stock_value:
        return stock_value
    return stock_corrections.get(stock_value.strip(), stock_value.strip())

labeled_df["stock_BO"] = labeled_df["stock_BO"].apply(apply_stock_corrections)

# ------------------------------------------------------------
# 10) Extract stock (final capital letter)
# ------------------------------------------------------------
def extract_stock(stock_bo):
    if not stock_bo:
        return None
    m = re.search(r"([A-Z])\s*$", stock_bo.strip())
    return m.group(1) if m else None

labeled_df["stock"] = labeled_df["stock_BO"].apply(extract_stock)

# ------------------------------------------------------------
# 11) Extract date
# ------------------------------------------------------------
def extract_date(text):
    if not text or not isinstance(text, str):
        return None
    m = re.search(r"(\d{1,2}/\d{1,2}/\d{2,4})", text)
    return m.group(1) if m else None

labeled_df["date"] = labeled_df["text_line"].apply(extract_date)

# ------------------------------------------------------------
# 12) Save output
# ------------------------------------------------------------
labeled_df.to_csv("pdf_lines_labeled_full_with_stock.csv", index=False)
print("✅ Finished create_datatable v7 → pdf_lines_labeled_full_with_stock.csv")

conn.close()