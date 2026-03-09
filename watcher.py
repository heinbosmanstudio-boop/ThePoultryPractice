import pdfplumber
import re
import os
import time
import shutil
import requests
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# ================== CONFIG ==================
FOLDER_TO_WATCH = os.environ.get("PDF_FOLDER", r"C:\Users\Hanlie\OneDrive\Desktop\poultry reports")
PROCESSED_FOLDER = Path(FOLDER_TO_WATCH) / "processed"

# Ensure processed folder exists
PROCESSED_FOLDER.mkdir(exist_ok=True)

# Lark / Bitable credentials from environment variables
APP_ID = os.environ["APP_ID"]
APP_SECRET = os.environ["APP_SECRET"]
APP_TOKEN = os.environ["APP_TOKEN"]
TABLE_ID = os.environ["TABLE_ID"]

# ================== LARK FUNCTIONS ==================
def get_tenant_token():
    """Generates a tenant token using App ID and Secret."""
    url = "https://open.larksuite.com/open-apis/auth/v3/tenant_access_token/internal"
    payload = {"app_id": APP_ID, "app_secret": APP_SECRET}
    res = requests.post(url, json=payload)
    res.raise_for_status()
    return res.json()["tenant_access_token"]

def insert_record(token, metadata, disease_row):
    """Inserts one disease block as a row in Lark."""
    url = f"https://open.larksuite.com/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    fields = {
        # report info
        "flddRmu5It": metadata["Lab Number"],
        "fldX0aY5kH": metadata["Sample Date"],
        "fldAtkUdXB": metadata["Client"],
        "fldqjccBNo": metadata["Farm Name"],
        "fldPy4ClNg": metadata["Address"],
        "fldiTix9Nk": metadata["Purpose of Sampling"],
        "fldxldCfYI": metadata["Species"],
        "fldRthV8jp": metadata["State Veterinarian"],

        # disease block
        "fldFQRj1wH": disease_row["Disease"],
        "fldDEVDTdt": disease_row["Avg Titre"],
        "fldyHnbTxJ": disease_row["CV %"],
        "fldxmt3lMo": disease_row["Interpretation"],
        "fldUqTEw6A": metadata.get("Original PDF", "")
    }

    payload = {"fields": fields}
    res = requests.post(url, headers=headers, json=payload)
    if res.status_code == 200:
        print(f"✅ Row inserted into Lark: {disease_row['Disease']}")
    else:
        print(f"❌ Failed to insert row: {res.status_code} {res.text}")

# ================== PDF EXTRACTION ==================
def extract_pdf(pdf_path):
    """Extracts header and disease blocks from a PDF."""
    with pdfplumber.open(pdf_path) as pdf:
        full_text = "\n".join(page.extract_text(x_tolerance=3, y_tolerance=3) or "" for page in pdf.pages)

    header = {
        "Lab Number": re.search(r"Lab Number\s*:\s*(\d+)", full_text).group(1) if re.search(r"Lab Number", full_text) else "",
        "Sample Date": re.search(r"Sample Date\s*:\s*([\d/]+)", full_text).group(1) if re.search(r"Sample Date", full_text) else "",
        "Client": re.search(r"Client\s*:\s*(.+?)(?=\s*Received Date|\n)", full_text).group(1).strip() if re.search(r"Client", full_text) else "",
        "Farm Name": re.search(r"Farm Name\s*:\s*(.+?)(?=\s*Report Date|\n)", full_text).group(1).strip() if re.search(r"Farm Name", full_text) else "",
        "Address": re.search(r"Address\s*:\s*(.+?)(?=\s*Purpose of Sampling|\n)", full_text).group(1).strip() if re.search(r"Address", full_text) else "",
        "Purpose of Sampling": re.search(r"Purpose of Sampling\s*:\s*(.+?)(?=\n|Species)", full_text).group(1).strip() if re.search(r"Purpose of Sampling", full_text) else "",
        "Species": re.search(r"Species\s*:\s*(.+?)(?=\n)", full_text).group(1).strip() if re.search(r"Species", full_text) else "",
        "State Veterinarian": re.search(r"State Veterinarian\s*:\s*(.+)", full_text).group(1).strip() if re.search(r"State Veterinarian", full_text) else "",
        "Original PDF": pdf_path.name
    }

    # Extract disease blocks
    serology_blocks = []
    avg_titres = re.findall(r"Avg Titre\s+(\d+)", full_text)
    cv_percents = re.findall(r"CV %\s+([\d.]+)", full_text)
    disease_matches = re.findall(r"Disease\s+(.+?)\s+Interpretation\s+([1A-Za-z]+)", full_text)

    for i in range(len(disease_matches)):
        block = {
            "Disease": disease_matches[i][0].strip(),
            "Avg Titre": avg_titres[i] if i < len(avg_titres) else "",
            "CV %": cv_percents[i] if i < len(cv_percents) else "",
            "Interpretation": disease_matches[i][1],
        }
        serology_blocks.append(block)

    return header, serology_blocks

# ================== WATCHER ==================
class PDFHandler(FileSystemEventHandler):
    def __init__(self, token):
        self.tenant_token = token

    def on_created(self, event):
        if event.is_directory or not event.src_path.lower().endswith(".pdf"):
            return

        pdf_path = Path(event.src_path)
        print(f"\n🚀 New PDF detected: {pdf_path.name}")
        time.sleep(2)  # give OneDrive/OS time to finish writing

        try:
            metadata, disease_blocks = extract_pdf(pdf_path)

            if not disease_blocks:
                print("❌ No disease blocks found in PDF.")
            else:
                for block in disease_blocks:
                    insert_record(self.tenant_token, metadata, block)

            # Move processed PDF
            shutil.move(str(pdf_path), PROCESSED_FOLDER / pdf_path.name)
            print(f"📁 PDF moved to processed folder")

        except Exception as e:
            print(f"❌ Error processing PDF {pdf_path.name}: {e}")

# ================== MAIN ==================
if __name__ == "__main__":
    tenant_token = get_tenant_token()
    print("✅ Tenant token obtained, watcher is running...")

    event_handler = PDFHandler(tenant_token)
    observer = Observer()
    observer.schedule(event_handler, FOLDER_TO_WATCH, recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
