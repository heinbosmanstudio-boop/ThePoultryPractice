import pdfplumber
import re
import os
import sys
import requests

# -----------------------------
# ENV VARIABLES (from Render)
# -----------------------------
APP_ID = os.environ["APP_ID"]
APP_SECRET = os.environ["APP_SECRET"]
APP_TOKEN = os.environ["APP_TOKEN"]
TABLE_ID = os.environ["TABLE_ID"]

# -----------------------------
# GET TENANT TOKEN
# -----------------------------
def get_tenant_token():
    url = "https://open.larksuite.com/open-apis/auth/v3/tenant_access_token/internal"
    payload = {"app_id": APP_ID, "app_secret": APP_SECRET}
    r = requests.post(url, json=payload)
    r.raise_for_status()
    return r.json()["tenant_access_token"]

# -----------------------------
# INSERT ROW INTO LARK
# -----------------------------
def insert_record(token, metadata, disease_row):
    url = f"https://open.larksuite.com/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    fields = {
        # Report info
        "flddRmu5It": metadata.get("Lab Number", ""),
        "fldX0aY5kH": metadata.get("Sample Date", ""),
        "fldAtkUdXB": metadata.get("Client", ""),
        "fldqjccBNo": metadata.get("Farm Name", ""),
        "fldPy4ClNg": metadata.get("Address", ""),
        "fldiTix9Nk": metadata.get("Purpose of Sampling", ""),
        "fldxldCfYI": metadata.get("Species", ""),
        "fldRthV8jp": metadata.get("State Veterinarian", ""),
        "fldUqTEw6A": metadata.get("Original PDF", ""),

        # Disease row
        "fld0PmKkh2": disease_row.get("Block", ""),
        "fldFQRj1wH": disease_row.get("Disease", ""),
        "fldDEVDTdt": disease_row.get("Avg Titre", ""),
        "fldyHnbTxJ": disease_row.get("CV %", ""),
        "fldxmt3lMo": disease_row.get("Interpretation", "")
    }

    payload = {"fields": fields}
    r = requests.post(url, headers=headers, json=payload)
    print(r.status_code, r.text)

# -----------------------------
# EXTRACT DATA FROM PDF
# -----------------------------
def extract_report(pdf_path):
    with pdfplumber.open(pdf_path) as pdf:
        text = "\n".join(page.extract_text(x_tolerance=3, y_tolerance=3) or "" for page in pdf.pages)

    # Header info
    header = {
        "Lab Number": re.search(r"Lab Number\s*:\s*(\d+)", text).group(1) if re.search(r"Lab Number", text) else "",
        "Sample Date": re.search(r"Sample Date\s*:\s*([\d/]+)", text).group(1) if re.search(r"Sample Date", text) else "",
        "Client": re.search(r"Client\s*:\s*(.+?)(?=\s*Received Date|\n)", text).group(1).strip() if re.search(r"Client", text) else "",
        "Farm Name": re.search(r"Farm Name\s*:\s*(.+?)(?=\s*Report Date|\n)", text).group(1).strip() if re.search(r"Farm Name", text) else "",
        "Address": re.search(r"Address\s*:\s*(.+?)(?=\s*Purpose of Sampling|\n)", text).group(1).strip() if re.search(r"Address", text) else "",
        "Purpose of Sampling": re.search(r"Purpose of Sampling\s*:\s*(.+?)(?=\n|Species)", text).group(1).strip() if re.search(r"Purpose of Sampling", text) else "",
        "Species": re.search(r"Species\s*:\s*(.+?)(?=\n)", text).group(1).strip() if re.search(r"Species", text) else "",
        "State Veterinarian": re.search(r"State Veterinarian\s*:\s*(.+)", text).group(1).strip() if re.search(r"State Veterinarian", text) else "",
        "Original PDF": os.path.basename(pdf_path)
    }

    # Serology / Disease blocks
    disease_matches = re.findall(r"Disease\s+(.+?)\s+Interpretation\s+([1A-Za-z]+)", text)
    avg_titres = re.findall(r"Avg Titre\s+(\d+)", text)
    cv_percents = re.findall(r"CV %\s+([\d.]+)", text)

    blocks = []
    for i, match in enumerate(disease_matches):
        block = {
            "Block": i + 1,
            "Disease": match[0].strip(),
            "Avg Titre": avg_titres[i] if i < len(avg_titres) else "",
            "CV %": cv_percents[i] if i < len(cv_percents) else "",
            "Interpretation": match[1]
        }
        blocks.append(block)

    return header, blocks

# -----------------------------
# MAIN
# -----------------------------
def main():
    if len(sys.argv) < 2:
        print("❌ Usage: python extract_to_lark.py <PDF_PATH>")
        return

    pdf_path = sys.argv[1]
    tenant_token = get_tenant_token()

    metadata, disease_rows = extract_report(pdf_path)
    print(f"📊 Extracted {len(disease_rows)} disease blocks from {pdf_path}")

    for row in disease_rows:
        insert_record(tenant_token, metadata, row)

if __name__ == "__main__":
    main()
