import os
import time
import threading
import requests
from pathlib import Path
from flask import Flask

app = Flask(__name__)

# -----------------------------
# CONFIGURATION
# -----------------------------

TENANT_ID = os.environ.get("TENANT_ID")
CLIENT_ID = os.environ.get("CLIENT_ID")
CLIENT_SECRET = os.environ.get("CLIENT_SECRET")

ONEDRIVE_OWNER_EMAIL = "hanlie.bosman@gmail.com"  # your OneDrive owner email
ONEDRIVE_FOLDER = "poultry reports"

PROCESSED_FILE = "processed_files.txt"


# -----------------------------
# FLASK ROUTE (for Render health check)
# -----------------------------
@app.route("/")
def home():
    return "Watcher running"


# -----------------------------
# GET GRAPH TOKEN
# -----------------------------
def get_token():
    print("Requesting Graph token...")
    url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
    data = {
        "client_id": CLIENT_ID,
        "scope": "https://graph.microsoft.com/.default",
        "client_secret": CLIENT_SECRET,
        "grant_type": "client_credentials",
    }
    r = requests.post(url, data=data)
    r.raise_for_status()
    token = r.json()["access_token"]
    print("Graph token obtained.")
    return token


# -----------------------------
# LIST FILES IN ONEDRIVE FOLDER
# -----------------------------
def get_files(token):
    headers = {"Authorization": f"Bearer {token}"}
    # Use /users/{email}/drive/root:/folder:/children
    url = f"https://graph.microsoft.com/v1.0/users/{ONEDRIVE_OWNER_EMAIL}/drive/root:/{ONEDRIVE_FOLDER}:/children"
    print(f"Checking OneDrive folder '{ONEDRIVE_FOLDER}'...")
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
    print(f"Downloading {filename}...")
    r = requests.get(download_url)
    r.raise_for_status()
    with open(filename, "wb") as f:
        f.write(r.content)
    print(f"Downloaded {filename}")
    return filename


# -----------------------------
# PROCESS PDF AND SEND TO LARK
# -----------------------------
def process_pdf(filepath):
    print(f"Processing PDF: {filepath}")

    # -----------------------------
    # Example: PDF parsing & Lark insert
    # Replace this with your real parser
    # -----------------------------
    # from extract_to_lark import parse_pdf, insert_to_lark
    # data_blocks = parse_pdf(filepath)
    # for block in data_blocks:
    #     insert_to_lark(block)
    # -----------------------------

    print(f"(Demo) PDF processed: {filepath}")


# -----------------------------
# WATCHER LOOP
# -----------------------------
def watcher_loop():
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
                process_pdf(path)
                save_processed(file["id"])

        except Exception as e:
            print("Watcher error:", e)

        time.sleep(60)  # check every 60 seconds


# -----------------------------
# MAIN
# -----------------------------
if __name__ == "__main__":
    print("Starting watcher thread...")
    thread = threading.Thread(target=watcher_loop, daemon=True)
    thread.start()

    port = int(os.environ.get("PORT", 10000))
    print(f"Starting web server on port {port}")
    app.run(host="0.0.0.0", port=port)
