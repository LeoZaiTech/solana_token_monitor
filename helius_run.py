import os
import sqlite3
import json
import requests
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
HELIUS_API_KEY = os.getenv("HELIUS_API_KEY")

# SQLite database initialization
DB_FILE = "solana_transactions.db"

def init_db():
    """Initialize the database schema for storing Solana transaction data."""
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS transactions (
            signature TEXT PRIMARY KEY,
            block_time INTEGER,
            fee INTEGER,
            error TEXT,
            log_messages TEXT,
            pre_balances TEXT,
            post_balances TEXT,
            token_transfers TEXT
        )
    ''')
    conn.commit()
    conn.close()

def fetch_transaction(signature):
    """Fetch transaction details from Helius API."""
    url = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
    headers = {"Content-Type": "application/json"}
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getTransaction",
        "params": [
            signature,
            {
                "encoding": "jsonParsed",
                "maxSupportedTransactionVersion": 0
            }
        ]
    }

    response = requests.post(url, headers=headers, json=payload)
    if response.status_code == 200:
        return response.json().get("result", {})
    else:
        print(f"Error fetching transaction: {response.text}")
        return None

def parse_transaction_data(tx_data):
    """Extract relevant data from the transaction response."""
    if not tx_data:
        return None

    signature = tx_data["transaction"]["signatures"][0]
    block_time = tx_data.get("blockTime", None)
    fee = tx_data["meta"]["fee"]
    error = tx_data["meta"].get("err", None)
    log_messages = json.dumps(tx_data["meta"].get("logMessages", []))
    pre_balances = json.dumps(tx_data["meta"].get("preBalances", []))
    post_balances = json.dumps(tx_data["meta"].get("postBalances", []))

    # Parse token transfers
    token_transfers = []
    for instruction in tx_data["meta"].get("innerInstructions", []):
        for token in instruction.get("instructions", []):
            if token.get("parsed"):
                transfer_info = token["parsed"]["info"]
                token_transfers.append({
                    "source": transfer_info.get("source"),
                    "destination": transfer_info.get("destination"),
                    "amount": transfer_info.get("amount"),
                    "mint": token["programId"]
                })
    token_transfers_json = json.dumps(token_transfers)

    return (signature, block_time, fee, str(error), log_messages, pre_balances, post_balances, token_transfers_json)

def save_transaction_to_db(tx_data):
    """Save parsed transaction data into the SQLite database."""
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''
        INSERT OR IGNORE INTO transactions 
        (signature, block_time, fee, error, log_messages, pre_balances, post_balances, token_transfers) 
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', tx_data)
    conn.commit()
    conn.close()

def main():
    init_db()

    # Sample transaction signature (replace with actual ones)
    sample_signature = "4JWQMMs63xBM3dGKUF29YZnyp6LMEJJGCACo6YBiU2toTqiUDPP79i35Ynct8f6ppCtnRGG7FM7DxomzmYCtuy6F"

    print(f"Fetching transaction: {sample_signature}")
    tx_data = fetch_transaction(sample_signature)

    if tx_data:
        parsed_data = parse_transaction_data(tx_data)
        if parsed_data:
            save_transaction_to_db(parsed_data)
            print("Transaction saved successfully.")
        else:
            print("No valid data to save.")
    else:
        print("Transaction fetch failed.")

if __name__ == "__main__":
    main()
