import os
import time
import threading
import requests
import pdfplumber
from pathlib import Path
from io import BytesIO
from urllib.parse import unquote
from urllib.request import urlopen

# -------------------------
# ENV VARIABLES
# -------------------------
APP_ID = os.environ["APP_ID"]
APP_SECRET = os.environ["APP_SECRET"]
APP_TOKEN = os.environ["APP_TOKEN"]
TABLE_ID = os.environ["TABLE_ID"]
ONEDRIVE_SHARE_LINK = os.environ["ONEDRIVE_SHARE_LINK"]  # folder link
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", 120))  # seconds

# -------------------------
# LARK TENANT TOKEN
# -------------------------
def get_tenant_token():
    url = "https://open.larksuite.com/open-apis/auth/v3/tenant_access_token/internal"
    payload = {"app_id": APP_ID, "app_secret": APP_SECRET}
    r = requests.post(url, json=payload)
    r.raise_for_status()
    data = r.json()
    return data["tenant_access_token"]

# -------------------------
# INSERT RECORD INTO LARK
# -------------------------
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
        "fldxmt3lMo": disease_row["interpretation"],
    }

    payload = {"fields": fields}
    r = requests.post(url, headers=headers, json=payload)
    print(r.status_code, r.text)

# -------------------------
# EXTRACT PDF DATA
# -------------------------
def extract_report_from_bytes(pdf_bytes):
    with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
        full_text = "\n".join(page.extract_text() or "" for page in pdf.pages)

    header = {
        "lab_number": header_get(full_text, "Lab Number"),
        "sample_date": header_get(full_text, "Sample Date"),
        "client": header_get(full_text, "Client"),
        "farm": header_get(full_text, "Farm Name"),
        "address": header_get(full_text, "Address"),
        "purpose": header_get(full_text, "Purpose of Sampling"),
        "species": header_get(full_text, "Species"),
        "state_vet": header_get(full_text, "State Veterinarian"),
    }

    diseases = []
    import re
    avg_titres = re.findall(r"Avg Titre\s+(\d+)", full_text)
    cv_percents = re.findall(r"CV %\s+([\d.]+)", full_text)
    disease_matches = re.findall(r"Disease\s+(.+?)\s+Interpretation\s+([1A]|Vaccinal|Positive|Negative)", full_text)

    for i, dm in enumerate(disease_matches):
        disease = {
            "disease": dm[0].strip(),
            "titre": avg_titres[i] if i < len(avg_titres) else "",
            "cv": cv_percents[i] if i < len(cv_percents) else "",
            "interpretation": dm[1],
        }
        diseases.append(disease)

    return header, diseases

def header_get(text, field):
    import re
    match = re.search(rf"{re.escape(field)}\s*:\s*(.+)", text)
    return match.group(1).strip() if match else ""

# -------------------------
# ONE DRIVE POLLING
# -------------------------
def poll_onedrive():
    tenant_token = get_tenant_token()
    processed_files = set()

    while True:
        try:
            # List files from OneDrive folder (basic approach)
            # Replace ?e=xxx with &download=1 to get direct download
            import requests, re

            folder_url = ONEDRIVE_SHARE_LINK
            html = requests.get(folder_url).text
            pdf_links = re.findall(r'href="(https://1drv.ms/[^\"]+\.pdf)"', html)
            
            for link in pdf_links:
                decoded_link = unquote(link)
                if decoded_link in processed_files:
                    continue
                print(f"📄 Processing {decoded_link}")
                pdf_bytes = urlopen(decoded_link).read()
                metadata, diseases = extract_report_from_bytes(pdf_bytes)
                for row in diseases:
                    insert_record(tenant_token, metadata, row)
                processed_files.add(decoded_link)

        except Exception as e:
            print("❌ Error in polling:", e)

        time.sleep(POLL_INTERVAL)

# -------------------------
# RUN WATCHER IN THREAD
# -------------------------
def start_watcher():
    thread = threading.Thread(target=poll_onedrive, daemon=True)
    thread.start()
    print("👀 Watcher thread started")

# -------------------------
# FLASK APP (dummy, keeps Render happy)
# -------------------------
from flask import Flask

app = Flask(__name__)

@app.route("/")
def index():
    return "PDF watcher running! Check logs for activity."

if __name__ == "__main__":
    start_watcher()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
