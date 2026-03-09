import pdfplumber
import re
import requests
from pathlib import Path
import time
import os
from urllib.parse import urlparse

# -----------------------------
# CONFIG
# -----------------------------
APP_ID = os.environ.get("APP_ID")
APP_SECRET = os.environ.get("APP_SECRET")
APP_TOKEN = os.environ.get("APP_TOKEN")
TABLE_ID = os.environ.get("TABLE_ID")

ONEDRIVE_SHARED_FOLDER_LINK = "https://1drv.ms/f/s/3a0d67070371eb0d/IgB_Tn54Jz5VSKF0cHc7LuySAXAVji6Pzci7QYbULrKnp2g?e=lAgbtb"

LOCAL_PDF_FOLDER = Path("/tmp/pdfs")
PROCESSED_FOLDER = LOCAL_PDF_FOLDER / "processed"
LOCAL_PDF_FOLDER.mkdir(parents=True, exist_ok=True)
PROCESSED_FOLDER.mkdir(exist_ok=True)

POLL_INTERVAL = 120  # seconds

# -----------------------------
# TENANT TOKEN
# -----------------------------
def get_tenant_token():
    url = "https://open.larksuite.com/open-apis/auth/v3/tenant_access_token/internal"
    payload = {"app_id": APP_ID, "app_secret": APP_SECRET}
    r = requests.post(url, json=payload)
    r.raise_for_status()
    token = r.json()["tenant_access_token"]
    print(f"✅ Tenant token obtained, expires in {r.json()['expire']}s")
    return token

# -----------------------------
# PDF EXTRACTION
# -----------------------------
def extract_report(pdf_path):
    with pdfplumber.open(pdf_path) as pdf:
        full_text = "\n".join(page.extract_text(x_tolerance=3, y_tolerance=3) or "" for page in pdf.pages)

    header = {
        "lab_number": re.search(r"Lab Number\s*:\s*(\d+)", full_text).group(1) if re.search(r"Lab Number", full_text) else "",
        "sample_date": re.search(r"Sample Date\s*:\s*([\d/]+)", full_text).group(1) if re.search(r"Sample Date", full_text) else "",
        "client": re.search(r"Client\s*:\s*(.+?)(?=\s*Received Date|\n)", full_text).group(1).strip() if re.search(r"Client", full_text) else "",
        "farm": re.search(r"Farm Name\s*:\s*(.+?)(?=\s*Report Date|\n)", full_text).group(1).strip() if re.search(r"Farm Name", full_text) else "",
        "address": re.search(r"Address\s*:\s*(.+?)(?=\s*Purpose of Sampling|\n)", full_text).group(1).strip() if re.search(r"Address", full_text) else "",
        "purpose": re.search(r"Purpose of Sampling\s*:\s*(.+?)(?=\n|Species)", full_text).group(1).strip() if re.search(r"Purpose of Sampling", full_text) else "",
        "species": re.search(r"Species\s*:\s*(.+?)(?=\n)", full_text).group(1).strip() if re.search(r"Species", full_text) else "",
        "state_vet": re.search(r"State Veterinarian\s*:\s*(.+)", full_text).group(1).strip() if re.search(r"State Veterinarian", full_text) else "",
    }

    avg_titres = re.findall(r"Avg Titre\s+(\d+)", full_text)
    cv_percents = re.findall(r"CV %\s+([\d.]+)", full_text)
    disease_matches = re.findall(r"Disease\s+(.+?)\s+Interpretation\s+([1A])", full_text)

    blocks = []
    for i in range(len(disease_matches)):
        block = {
            **header,
            "disease": disease_matches[i][0].strip(),
            "titre": avg_titres[i] if i < len(avg_titres) else "",
            "cv": cv_percents[i] if i < len(cv_percents) else "",
            "interpretation": disease_matches[i][1],
        }
        blocks.append(block)

    return blocks

# -----------------------------
# INSERT INTO LARK
# -----------------------------
def insert_record(token, metadata, disease_row):
    url = f"https://open.larksuite.com/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    fields = {
        "flddRmu5It": metadata["lab_number"],
        "fldX0aY5kH": metadata["sample_date"],
        "fldAtkUdXB": metadata["client"],
        "fldqjccBNo": metadata["farm"],
        "fldPy4ClNg": metadata["address"],
        "fldiTix9Nk": metadata["purpose"],
        "fldxldCfYI": metadata["species"],
        "fldRthV8jp": metadata["state_vet"],
        "fldFQRj1wH": disease_row["disease"],
        "fldDEVDTdt": disease_row["titre"],
        "fldyHnbTxJ": disease_row["cv"],
        "fldxmt3lMo": disease_row["interpretation"]
    }

    r = requests.post(url, headers=headers, json={"fields": fields})
    print(r.status_code, r.text)

# -----------------------------
# DOWNLOAD PDF
# -----------------------------
def download_pdf(file_url, dest_folder):
    filename = urlparse(file_url).path.split("/")[-1]
    local_path = dest_folder / filename
    direct_url = file_url.replace("1drv.ms", "1drv.ws").replace("?e=", "?download=")
    r = requests.get(direct_url)
    r.raise_for_status()
    with open(local_path, "wb") as f:
        f.write(r.content)
    return local_path

# -----------------------------
# GET FILES FROM ONEDRIVE FOLDER
# -----------------------------
def list_onedrive_pdfs(shared_folder_link):
    # Transform shared link into "embed" JSON endpoint
    embed_url = shared_folder_link.replace("1drv.ms", "1drv.ws").replace("?e=", "?embed=json&")
    r = requests.get(embed_url)
    r.raise_for_status()
    data = r.json()
    pdf_urls = []

    # OneDrive JSON structure may vary, but files usually in 'value' list
    for item in data.get("value", []):
        name = item.get("name", "")
        if name.lower().endswith(".pdf"):
            pdf_urls.append(item.get("webUrl"))
    return pdf_urls

# -----------------------------
# WATCHER LOOP
# -----------------------------
def watcher():
    print("👀 Watcher running. Polling OneDrive folder...")
    processed_files = set(f.name for f in PROCESSED_FOLDER.glob("*.pdf"))
    tenant_token = get_tenant_token()

    while True:
        try:
            pdf_urls = list_onedrive_pdfs(ONEDRIVE_SHARED_FOLDER_LINK)

            for url in pdf_urls:
                filename = urlparse(url).path.split("/")[-1]
                if filename in processed_files:
                    continue

                print(f"🚀 New PDF detected: {filename}")
                pdf_path = download_pdf(url, LOCAL_PDF_FOLDER)
                rows = extract_report(pdf_path)
                for row in rows:
                    insert_record(tenant_token, row, row)

                pdf_path.rename(PROCESSED_FOLDER / pdf_path.name)
                processed_files.add(filename)
                print(f"📁 PDF moved to processed folder: {filename}")

        except Exception as e:
            print(f"❌ Error: {e}")

        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    watcher()
