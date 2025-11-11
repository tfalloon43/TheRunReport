import os
import requests
from bs4 import BeautifulSoup

# --- Configuration ---
BASE_URL = "https://wdfw.wa.gov/fishing/management/hatcheries/escapement"  # main site for PDFs

# Detect user's Desktop (cross-platform)
desktop_path = os.path.join(os.path.expanduser("~"), "Desktop")

# Create a folder called "RunReport_PDFs" on the Desktop
pdf_folder = os.path.join(desktop_path, "RunReport_PDFs")
os.makedirs(pdf_folder, exist_ok=True)

print(f"📁 Saving PDFs to: {pdf_folder}")

# --- Step 1: Fetch the page ---
response = requests.get(BASE_URL)
if response.status_code != 200:
    raise Exception(f"Failed to fetch {BASE_URL} (status {response.status_code})")

soup = BeautifulSoup(response.text, "html.parser")

# --- Step 2: Find all PDF links ---
pdf_links = []
for link in soup.find_all("a", href=True):
    href = link["href"]
    if href.lower().endswith(".pdf"):
        if href.startswith("http"):
            pdf_links.append(href)
        else:
            pdf_links.append(requests.compat.urljoin(BASE_URL, href))

print(f"🔗 Found {len(pdf_links)} PDF links")

# --- Step 3: Download PDFs to Desktop folder ---
for pdf_url in pdf_links:
    filename = os.path.basename(pdf_url)
    filepath = os.path.join(pdf_folder, filename)

    if os.path.exists(filepath):
        print(f"⏩ Skipping (already exists): {filename}")
        continue

    try:
        #print(f"⬇️  Downloading: {filename}")
        pdf_data = requests.get(pdf_url, timeout=15)
        pdf_data.raise_for_status()
        with open(filepath, "wb") as f:
            f.write(pdf_data.content)
    except Exception as e:
        print(f"⚠️  Failed to download {filename}: {e}")

print("✅ All PDFs downloaded successfully!")