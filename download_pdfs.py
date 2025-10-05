import os
import requests
from bs4 import BeautifulSoup

# URL of the page where PDFs are listed
base_url = "https://wdfw.wa.gov/fishing/management/hatcheries/escapement"  # <-- example, update with real URL
pdf_folder = "pdf_reports"

# Ensure folder exists
os.makedirs(pdf_folder, exist_ok=True)

# Step 1: Fetch the page
response = requests.get(base_url)
if response.status_code != 200:
    raise Exception(f"Failed to fetch {base_url}")

soup = BeautifulSoup(response.text, "html.parser")

# Step 2: Find all PDF links
pdf_links = []
for link in soup.find_all("a", href=True):
    href = link["href"]
    if href.lower().endswith(".pdf"):
        # Ensure absolute URL
        if href.startswith("http"):
            pdf_links.append(href)
        else:
            pdf_links.append(requests.compat.urljoin(base_url, href))

# Step 3: Download PDFs
for pdf_url in pdf_links:
    filename = os.path.basename(pdf_url)
    filepath = os.path.join(pdf_folder, filename)

    if os.path.exists(filepath):
        print(f"Skipping (already exists): {filename}")
        continue

    # print(f"Downloading: {filename}")
    pdf_data = requests.get(pdf_url)
    with open(filepath, "wb") as f:
        f.write(pdf_data.content)

print("All PDFs downloaded!")