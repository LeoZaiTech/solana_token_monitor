import os
import requests
import sqlite3
import json
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
SOLSCAN_API_KEY = os.getenv("SOLSCAN_API_KEY")

def fetch_tokens_from_solscan(limit=100):
    """Fetches a list of Solana tokens from Solscan API."""
    url = f"https://pro-api.solscan.io/v1/token/list?limit={limit}"
    headers = {
        "accept": "application/json",
        "token": SOLSCAN_API_KEY
    }

    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        data = response.json()
        if "data" in data:
            return data["data"]
        else:
            print("No token data found.")
    else:
        print(f"Error: {response.status_code}, {response.text}")
    return None

if __name__ == "__main__":
    tokens = fetch_tokens_from_solscan(10)
    if tokens:
        print(json.dumps(tokens, indent=4))
