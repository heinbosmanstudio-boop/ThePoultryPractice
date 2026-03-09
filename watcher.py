# watcher.py
import os
import time
import threading
import requests
from extract_pdf import extract_pdf  # <- your custom PDF parser
from flask import Flask

app = Flask(__name__)

# ------------------------
# Azure / OneDrive credentials
# ------------------------
TENANT_ID = os.environ["TENANT_ID"]
CLIENT_ID = os.environ["CLIENT_ID"]
CLIENT_SECRET = os.environ["CLIENT_SECRET"]

# ------------------------
# Lark credentials
# ------------------------
LARK_APP_TOKEN = os.environ["LARK_APP_TOKEN"]
LARK_TABLE_ID = os.environ["LARK_TABLE_ID"]

PROCESSED_FILE = "processed_files.txt"

# ------------------------
# Flask route for health check
# ------------------------
@app.route("/")
def home():
    return "Watcher running"


# ------------------------
# Get Microsoft Graph token
# ------------------------
def get_token():
    url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
    data = {
        "client_id": CLIENT_ID,
        "scope": "https://graph.microsoft.com/.default",
        "client_secret": CLIENT_SECRET,
        "grant_type": "client_credentials",
    }
    r = requests.post(url, data=data)
    r.raise_for_status()
    return r.json()["access_token"]


# ------------------------
# List files in OneDrive folder "Reports"
# ------------------------
def get_files(token):
    headers = {"Authorization": f"Bearer {token}"}
    url = "https://graph.microsoft.com/v1.0/me/drive/root:/Reports:/children"
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    return r.json()["value"]


# ------------------------
# Track processed PDFs
# ------------------------
def load_processed():
    if not os.path.exists(PROCESSED_FILE):
        return set()
    with open(PROCESSED_FILE) as f:
        return set(f.read().splitlines())


def save_processed(file_id):
    with open(PROCESSED_FILE, "a") as f:
        f.write(file_id + "\n")


# ------------------------
# Download PDF locally
# ------------------------
def download_file(file):
    download_url = file["@microsoft.graph.downloadUrl"]
    r = requests.get(download_url)
    filename = file["name"]
    with open(filename, "wb") as f:
        f.write(r.content)
    return filename


# ------------------------
# Insert row into Lark
# ------------------------
def insert_record(metadata, disease_row):
    url = f"https://open.larksuite.com/open-apis/bitable/v1/apps/{LARK_APP_TOKEN}/tables/{LARK_TABLE_ID}/records"
    headers = {
        "Authorization": f"Bearer {LARK_APP_TOKEN}",
        "Content-Type": "application/json"
    }

    fields = {
        # metadata fields
        "flddRmu5It": metadata["lab_number"],
        "fldX0aY5kH": metadata["sample_date"],
        "fldAtkUdXB": metadata["client"],
        "fldqjccBNo": metadata["farm"],
        "fldPy4ClNg": metadata["address"],
        "fldiTix9Nk": metadata["purpose"],
        "fldxldCfYI": metadata["species"],
        "fldRthV8jp": metadata["state_vet"],

        # disease row fields
        "fldFQRj1wH": disease_row["disease"],
        "fldDEVDTdt": disease_row["titre"],
        "fldyHnbTxJ": disease_row["cv"],
        "fldxmt3lMo": disease_row["interpretation"]
    }

    payload = {"fields": fields}
    r = requests.post(url, headers=headers, json=payload)
    print(r.status_code, r.text)


# ------------------------
# Watcher loop
# ------------------------
def watcher_loop():
    print("Watcher loop started")
    while True:
        try:
            print("Checking OneDrive...")
            token = get_token()
            files = get_files(token)
            processed = load_processed()

            for file in files:
                if not file["name"].lower().endswith(".pdf"):
                    continue
                if file["id"] in processed:
                    continue

                print("New PDF detected:", file["name"])
                path = download_file(file)

                # extract PDF data
                metadata, disease_rows = extract_pdf(path)

                # insert each disease row into Lark
                for row in disease_rows:
                    insert_record(metadata, row)

                save_processed(file["id"])

        except Exception as e:
            print("Watcher error:", e)

        time.sleep(60)  # check every 60 seconds


# ------------------------
# Main
# ------------------------
if __name__ == "__main__":
    print("Starting watcher thread...")
    thread = threading.Thread(target=watcher_loop)
    thread.start()

    port = int(os.environ.get("PORT", 10000))
    print("Starting web server on port", port)
    app.run(host="0.0.0.0", port=port)
