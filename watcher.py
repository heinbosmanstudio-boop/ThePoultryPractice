import os
import time
import threading
import requests
from extract_pdf import extract_pdf
from flask import Flask

app = Flask(__name__)

# -----------------------------
# Azure App Credentials
# -----------------------------
TENANT_ID = os.environ.get("TENANT_ID")
CLIENT_ID = os.environ.get("CLIENT_ID")
CLIENT_SECRET = os.environ.get("CLIENT_SECRET")

# -----------------------------
# OneDrive folder to watch
# -----------------------------
ONEDRIVE_FOLDER = "Reports"  # the folder inside OneDrive
PROCESSED_FILE = "processed_files.txt"

# -----------------------------
# Lark integration
# -----------------------------
# Set these environment variables in Render or locally
APP_ID = os.environ.get("APP_ID")
APP_SECRET = os.environ.get("APP_SECRET")
APP_TOKEN = os.environ.get("APP_TOKEN")
TABLE_ID = os.environ.get("TABLE_ID")

# -----------------------------
# Flask route for status
# -----------------------------
@app.route("/")
def home():
    return "Watcher running"


# -----------------------------
# Get Microsoft Graph token
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
    return r.json()["access_token"]


# -----------------------------
# Get files from OneDrive folder
# -----------------------------
def get_files(token):
    headers = {"Authorization": f"Bearer {token}"}
    url = f"https://graph.microsoft.com/v1.0/me/drive/root:/{ONEDRIVE_FOLDER}:/children"
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    return r.json()["value"]


# -----------------------------
# Process tracking
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
# Download PDF
# -----------------------------
def download_file(file):
    download_url = file["@microsoft.graph.downloadUrl"]
    r = requests.get(download_url)
    filename = file["name"]
    with open(filename, "wb") as f:
        f.write(r.content)
    print("Downloaded:", filename)
    return filename


# -----------------------------
# Main watcher loop
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

                print("New PDF detected:", file["name"])
                filepath = download_file(file)

                # --- Process PDF and send to Lark ---
                extract_pdf_to_lark(filepath, APP_ID, APP_SECRET, APP_TOKEN, TABLE_ID)

                save_processed(file["id"])
                print("Processed and sent to Lark:", file["name"])

        except Exception as e:
            print("Watcher error:", e)

        time.sleep(60)  # check every 60 seconds


# -----------------------------
# Main entry
# -----------------------------
if __name__ == "__main__":
    print("Starting watcher thread...")
    thread = threading.Thread(target=watcher_loop)
    thread.daemon = True
    thread.start()

    port = int(os.environ.get("PORT", 10000))
    print("Starting web server on port", port)
    app.run(host="0.0.0.0", port=port)
