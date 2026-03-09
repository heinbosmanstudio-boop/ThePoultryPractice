import requests

import os

APP_ID = os.environ["APP_ID"]
APP_SECRET = os.environ["APP_SECRET"]
APP_TOKEN = os.environ["APP_TOKEN"]
TABLE_ID = os.environ["TABLE_ID"]


# -----------------------------
# GET TENANT TOKEN
# -----------------------------

def get_tenant_token():

    url = "https://open.larksuite.com/open-apis/auth/v3/tenant_access_token/internal"

    payload = {
        "app_id": APP_ID,
        "app_secret": APP_SECRET
    }

    r = requests.post(url, json=payload)
    return r.json()["tenant_access_token"]


# -----------------------------
# INSERT ROW INTO LARK
# -----------------------------

def insert_record(token, metadata, disease_row):

    url = f"https://open.larksuite.com/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records"

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    fields = {

    "Lab Number": metadata["lab_number"],
    "Sample Date": metadata["sample_date"],
    "Client": metadata["client"],
    "Farm Name": metadata["farm"],
    "Address": metadata["address"],
    "Purpose of Sampling": metadata["purpose"],
    "Species": metadata["species"],
    "State Veterinarian": metadata["state_vet"],

    "Disease": disease_row["disease"],
    "Avg Titre": str(disease_row["titre"]),
    "CV %": str(disease_row["cv"]),
    "Interpretation": disease_row["interpretation"]
}

    payload = {
        "fields": fields
    }

    r = requests.post(url, headers=headers, json=payload)

    print(r.status_code, r.text)


# -----------------------------
# MAIN
# -----------------------------

def main():

    tenant_token = get_tenant_token()

    # Example metadata (from your parser)
    metadata = {
        "lab_number": "LAB123",
        "sample_date": "2026-03-09",
        "client": "ABC Poultry",
        "farm": "Green Farm",
        "address": "Pretoria",
        "purpose": "Monitoring",
        "species": "Chicken",
        "state_vet": "Dr Smith"
    }

    # Example diseases extracted from PDF
    diseases = [

        {
            "disease": "Newcastle",
            "titre": 2400,
            "cv": 10.5,
            "interpretation": "Positive"
        },

        {
            "disease": "IBD",
            "titre": 5200,
            "cv": 8.2,
            "interpretation": "Vaccinal"
        }
    ]

    for row in diseases:
        insert_record(tenant_token, metadata, row)


if __name__ == "__main__":
    main()
