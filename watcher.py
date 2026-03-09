import os
import time
import threading
import requests
from flask import Flask

app = Flask(__name__)

TENANT_ID = os.environ["TENANT_ID"]
CLIENT_ID = os.environ["CLIENT_ID"]
CLIENT_SECRET = os.environ["CLIENT_SECRET"]

PROCESSED_FILE = "processed_files.txt"


@app.route("/")
def home():
    return "Watcher running"


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


def get_files(token):

    headers = {"Authorization": f"Bearer {token}"}

    url = "https://graph.microsoft.com/v1.0/users/me/drive/root:/Reports:/children"

    r = requests.get(url, headers=headers)

    print("Graph response:", r.status_code)

    if r.status_code != 200:
        print(r.text)
        return []

    return r.json()["value"]


def load_processed():
    if not os.path.exists(PROCESSED_FILE):
        return set()

    with open(PROCESSED_FILE) as f:
        return set(f.read().splitlines())


def save_processed(file_id):
    with open(PROCESSED_FILE, "a") as f:
        f.write(file_id + "\n")


def download_file(file):
    download_url = file["@microsoft.graph.downloadUrl"]
    r = requests.get(download_url)

    filename = file["name"]

    with open(filename, "wb") as f:
        f.write(r.content)

    return filename


def process_pdf(filepath):
    print("Processing:", filepath)

    # your existing PDF parser goes here
    # parse_pdf(filepath)
    # send_to_lark(data)


def watcher_loop():

    print("Watcher started")

    while True:
        try:

            print("Checking OneDrive folder...")

            token = get_token()

            files = get_files(token)

            print("Files found:", len(files))

            processed = load_processed()

            for file in files:

                print("Found file:", file["name"])

                if not file["name"].lower().endswith(".pdf"):
                    continue

                if file["id"] in processed:
                    continue

                print("New PDF detected:", file["name"])

                path = download_file(file)

                process_pdf(path)

                save_processed(file["id"])

        except Exception as e:
            print("Watcher error:", e)

        time.sleep(60)


if __name__ == "__main__":

    print("Starting watcher thread...")

    thread = threading.Thread(target=watcher_loop)
    thread.start()

    port = int(os.environ.get("PORT", 10000))

    print("Starting web server on port", port)

    app.run(host="0.0.0.0", port=port)
