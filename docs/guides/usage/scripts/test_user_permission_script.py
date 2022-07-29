import json

import requests
from requests.auth import HTTPBasicAuth

username = None
password = None
BASE_URL = "http://0.0.0.0:8000"
DOMAIN = "domain"
DATASET = "dataset"


def fetch_user_token():
    auth = HTTPBasicAuth(username, password)

    payload = {
        "grant_type": "client_credentials",
        "client_id": username,
    }

    headers = None

    response = requests.post(
        BASE_URL + "/oauth2/token", auth=auth, headers=headers, json=payload
    )

    return json.loads(response.content.decode("utf-8"))["access_token"]


cookie = ""


def fetch_dataset(token: str, domain: str, dataset: str):
    post_url = f"{BASE_URL}/datasets/{domain}/{dataset}/query"
    headers = {"Accept": "text/html...", "cookie": f"rat={token}"}
    query = {"limit": "10"}
    response = requests.post(post_url, data=json.dumps(query), headers=headers)
    return response.status_code, json.loads(response.content.decode("utf-8"))


fetch_dataset(cookie, DOMAIN, DATASET)
