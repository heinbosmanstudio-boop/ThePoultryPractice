import os
import time
import threading
import requests
from pathlib import Path
from extract_pdf import extract_diseases_from_pdf  # your PDF parser

# -----------------------------
# CONFIGURATION FROM ENV
# -----------------------------
TENANT_ID = os.environ["TENANT_ID"]
CLIENT_ID = os.environ["CLIENT_ID"]
CLIENT_SECRET = os.environ["CLIENT_SECRET"]
ONEDRIVE_OWNER_EMAIL = os.environ.get("ONEDRIVE_OWNER_EMAIL", "hanlie.bosman@gmail.com")
ONEDRIVE_FOLDER = os.environ.get("ONEDRIVE_FOLDER", "poultry reports")
PROCESSED_FILE = "processed_files.txt"

# Lark configuration
APP_TOKEN = os.environ["LARK_APP_TOKEN"]
TABLE_ID = os.environ["LARK_TABLE_ID"]

# Lark field IDs
LARK_FIELDS = {
    "lab_number": os.environ["LARK_FLD_LAB_NUMBER"],
    "sample_date": os.environ["LARK_FLD_SAMPLE_DATE"],
    "client": os.environ["LARK_FLD_CLIENT"],
    "farm": os.environ["LARK_FLD_FARM"],
    "address": os.environ["LARK_FLD_ADDRESS"],
    "purpose": os.environ["LARK_FLD_PURPOSE"],
    "species": os.environ["LARK_FLD_SPECIES"],
    "state_vet": os.environ["LARK_FLD_STATE_VET"],
    "disease": os.environ["LARK_FLD_DISEASE"],
    "titre": os.environ["LARK_FLD_TITRE"],
    "cv": os.environ["LARK_FLD_CV"],
    "interpretation": os.environ["LARK_FLD_INTERPRETATION"]
}

# -----------------------------
# GET GRAPH TOKEN
# -----------------------------
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

# -----------------------------
# LIST FILES IN ONEDRIVE FOLDER
# -----------------------------
def get_files(token):
    headers = {"Authorization": f"Bearer {token}"}
    url = f"https://graph.microsoft.com/v1.0/users/{ONEDRIVE_OWNER_EMAIL}/drive/root:/{ONEDRIVE_FOLDER}:/children"
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    return r.json()["value"]

# -----------------------------
# PROCESSED FILES TRACKER
# -----------------------------
def load_processed():
    if not os.path.exists(PROCESSED_FILE):
        return set()
    with open(PROCESSED_FILE) as f:
        return set(f.read().splitlines())

def save_processed(file_id):
    with open(PROCESSED_FILE, "a") as f:
        f.write(file_id + "\n")

# -----------------------------
# DOWNLOAD FILE FROM ONEDRIVE
# -----------------------------
def download_file(file):
    download_url = file["@microsoft.graph.downloadUrl"]
    filename = Path(file["name"])
    r = requests.get(download_url)
    r.raise_for_status()
    with open(filename, "wb") as f:
        f.write(r.content)
    return filename

# -----------------------------
# INSERT ROW INTO LARK
# -----------------------------
def insert_to_lark(metadata, disease_row):
    url = f"https://open.larksuite.com/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records"
    headers = {"Authorization": f"Bearer {APP_TOKEN}", "Content-Type": "application/json"}
    fields = {
        LARK_FIELDS["lab_number"]: metadata["lab_number"],
        LARK_FIELDS["sample_date"]: metadata["sample_date"],
        LARK_FIELDS["client"]: metadata["client"],
        LARK_FIELDS["farm"]: metadata["farm"],
        LARK_FIELDS["address"]: metadata["address"],
        LARK_FIELDS["purpose"]: metadata["purpose"],
        LARK_FIELDS["species"]: metadata["species"],
        LARK_FIELDS["state_vet"]: metadata["state_vet"],
        LARK_FIELDS["disease"]: disease_row["disease"],
        LARK_FIELDS["titre"]: disease_row["titre"],
        LARK_FIELDS["cv"]: disease_row["cv"],
        LARK_FIELDS["interpretation"]: disease_row["interpretation"]
    }
    payload = {"fields": fields}
    r = requests.post(url, headers=headers, json=payload)
    print(r.status_code, r.text)

# -----------------------------
# WATCHER LOOP
# -----------------------------
def watcher_loop():
    print("Watcher loop started")
    while True:
        try:
            token = get_token()
            files = get_files(token)
            processed = load_processed()

            for file in files:
                if not file["name"].lower().endswith(".pdf"):
                    continue
                if file["id"] in processed:
                    continue

                print(f"New PDF detected: {file['name']}")
                path = download_file(file)

                # Use your PDF parser
                metadata, diseases = extract_diseases_from_pdf(path)

                for row in diseases:
                    insert_to_lark(metadata, row)

                save_processed(file["id"])

        except Exception as e:
            print("Watcher error:", e)

        time.sleep(60)

# -----------------------------
# MAIN
# -----------------------------
if __name__ == "__main__":
    print("Starting watcher thread...")
    thread = threading.Thread(target=watcher_loop, daemon=True)
    thread.start()
    
    # Simple health endpoint for Render/Azure
    from flask import Flask
    app = Flask(__name__)

    @app.route("/")
    def home():
        return "Watcher running"

    port = int(os.environ.get("PORT", 10000))
    print(f"Starting web server on port {port}")
    app.run(host="0.0.0.0", port=port)
