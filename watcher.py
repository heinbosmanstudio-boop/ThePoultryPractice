import os
import time
import threading
import requests
from flask import Flask
from extract_pdf import extract_pdf  # your existing PDF parser/extractor

app = Flask(__name__)

# ---------------------------------------
# CONFIGURATION
# ---------------------------------------

# Azure app credentials (heinbosmanstudio@gmail.com)
TENANT_ID = os.environ["TENANT_ID"]
CLIENT_ID = os.environ["CLIENT_ID"]
CLIENT_SECRET = os.environ["CLIENT_SECRET"]

# OneDrive shared folder link
ONEDRIVE_SHARE_LINK = "https://1drv.ms/f/c/0e6ea992be415296/IgCbNFYWg4GSRrpBDCnYIIhLAVlvwwOPSIhzlw0eEln3_gg?e=2Szyuc"

# Track processed files
PROCESSED_FILE = "processed_files.txt"

# Polling interval in seconds
POLL_INTERVAL = 60

# ---------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------

def get_token():
    """Request an Azure AD token using client credentials."""
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

def share_link_to_encoded_id(link):
    """Convert OneDrive share link to encoded ID for Graph API."""
    import base64
    encoded = base64.urlsafe_b64encode(link.encode("utf-8")).decode("utf-8").rstrip("=")
    return f"u!{encoded}"

def get_shared_folder_id(token, share_link):
    """Get folder ID from OneDrive shared link."""
    headers = {"Authorization": f"Bearer {token}"}
    url = f"https://graph.microsoft.com/v1.0/shares/{share_link_to_encoded_id(share_link)}/driveItem"
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    return r.json()["id"]

def list_folder_files(token, folder_id):
    """List files in OneDrive folder."""
    headers = {"Authorization": f"Bearer {token}"}
    url = f"https://graph.microsoft.com/v1.0/me/drive/items/{folder_id}/children"
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    return r.json().get("value", [])

def load_processed():
    if not os.path.exists(PROCESSED_FILE):
        return set()
    with open(PROCESSED_FILE) as f:
        return set(f.read().splitlines())

def save_processed(file_id):
    with open(PROCESSED_FILE, "a") as f:
        f.write(file_id + "\n")

def download_file(file):
    """Download PDF from OneDrive to local."""
    download_url = file["@microsoft.graph.downloadUrl"]
    filename = file["name"]
    r = requests.get(download_url)
    r.raise_for_status()
    with open(filename, "wb") as f:
        f.write(r.content)
    return filename

# ---------------------------------------
# WATCHER LOOP
# ---------------------------------------

def watcher_loop():
    print("Watcher loop started")
    while True:
        try:
            token = get_token()
            folder_id = get_shared_folder_id(token, ONEDRIVE_SHARE_LINK)
            files = list_folder_files(token, folder_id)
            processed = load_processed()

            for file in files:
                if not file["name"].lower().endswith(".pdf"):
                    continue
                if file["id"] in processed:
                    continue

                print("New PDF detected:", file["name"])
                path = download_file(file)

                # Process PDF using your existing extractor
                extract_pdf(path)

                save_processed(file["id"])

        except Exception as e:
            print("Watcher error:", e)

        time.sleep(POLL_INTERVAL)

# ---------------------------------------
# FLASK ROUTES
# ---------------------------------------

@app.route("/")
def home():
    return "Watcher is running"

# ---------------------------------------
# MAIN
# ---------------------------------------

if __name__ == "__main__":
    print("Starting watcher thread...")
    thread = threading.Thread(target=watcher_loop, daemon=True)
    thread.start()

    port = int(os.environ.get("PORT", 10000))
    print("Starting web server on port", port)
    app.run(host="0.0.0.0", port=port)
