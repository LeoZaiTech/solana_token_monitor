import os
import sqlite3
import json
import requests
import logging
import asyncio
import websockets
from datetime import datetime, timezone
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
HELIUS_API_KEY = os.getenv("HELIUS_API_KEY")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")

# SQLite database initialization
DB_FILE = "solana_transactions.db"

# Configure logging for detailed debugging
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")

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
            token_transfers TEXT,
            deployer_address TEXT,
            holder_count INTEGER,
            sniper_count INTEGER,
            insider_count INTEGER,
            buy_sell_ratio REAL,
            high_holder_count INTEGER
        )
    ''')
    conn.commit()
    conn.close()
    logging.debug("Database initialized successfully.")

def fetch_transaction(signature):
    """Fetch transaction details from Helius API."""
    logging.debug(f"Fetching transaction for signature: {signature}")
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
        logging.debug(f"Transaction data retrieved for {signature}")
        return response.json().get("result", {})
    else:
        logging.error(f"Error fetching transaction: {response.text}")
        return None

def analyze_holders(token_transfers):
    """Analyze token holders and identify potential snipers or insiders."""
    sniper_wallets = set(os.getenv("SNIPER_WALLETS", "").split(","))
    insider_wallets = set(os.getenv("INSIDER_WALLETS", "").split(","))

    holder_count = len(set(tx["destination"] for tx in token_transfers))
    sniper_count = sum(1 for tx in token_transfers if tx["destination"] in sniper_wallets)
    insider_count = sum(1 for tx in token_transfers if tx["destination"] in insider_wallets)

    high_holders = {}
    for tx in token_transfers:
        high_holders[tx["destination"]] = high_holders.get(tx["destination"], 0) + int(tx["amount"])

    high_holder_count = sum(1 for amount in high_holders.values() if amount > 0.08 * sum(high_holders.values()))

    logging.debug(f"Holders analyzed: {holder_count}, Snipers: {sniper_count}, Insiders: {insider_count}, High Holders: {high_holder_count}")
    return holder_count, sniper_count, insider_count, high_holder_count

def parse_transaction_data(tx_data):
    """Extract relevant data from the transaction response."""
    if not tx_data:
        logging.warning("No transaction data available to parse.")
        return None

    signature = tx_data["transaction"]["signatures"][0]
    block_time = tx_data.get("blockTime", None)
    fee = tx_data["meta"]["fee"]
    error = tx_data["meta"].get("err", None)
    log_messages = json.dumps(tx_data["meta"].get("logMessages", []))
    pre_balances = json.dumps(tx_data["meta"].get("preBalances", []))
    post_balances = json.dumps(tx_data["meta"].get("postBalances", []))

    token_transfers = []
    deployer_address = tx_data["transaction"]["message"]["accountKeys"][0]["pubkey"]
    buy_count, sell_count = 0, 0

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
                if transfer_info.get("destination") == deployer_address:
                    sell_count += 1
                else:
                    buy_count += 1

    buy_sell_ratio = round((buy_count / max(sell_count, 1)) * 100, 2)

    holder_count, sniper_count, insider_count, high_holder_count = analyze_holders(token_transfers)
    token_transfers_json = json.dumps(token_transfers)

    logging.debug(f"Parsed transaction data for signature {signature}")
    return (signature, block_time, fee, str(error), log_messages, pre_balances, post_balances,
            token_transfers_json, deployer_address, holder_count, sniper_count, insider_count,
            buy_sell_ratio, high_holder_count)

def notify_discord(tx_data):
    """Send a detailed notification to Discord."""
    logging.info(f"Preparing to send notification for: {tx_data[0]}")
    
    message = f"🚨 New Token Transaction Alert! Signature: {tx_data[0]}"
    payload = {"content": message}
    response = requests.post(DISCORD_WEBHOOK_URL, json=payload)

    logging.info(f"Discord Response: {response.status_code}, {response.text}")

    if response.status_code == 204:
        logging.info("Notification sent successfully.")
    else:
        logging.error(f"Error sending notification: {response.text}")

async def monitor_tokens():
    """Subscribe to token launches and monitor transactions via Helius WebSocket API."""
    url = f"wss://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
    while True:
        try:
            async with websockets.connect(url) as websocket:
                logging.info("Subscribed to pump.fun token launches...")

                request = {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "accountSubscribe",
                    "params": [
                        "CM78CPUeXjn8o3yroDHxUtKsZZgoy4GPKPPXfouKNH12",
                        {"encoding": "jsonParsed", "commitment": "finalized"}
                    ]
                }

                await websocket.send(json.dumps(request))
                while True:
                    response = await websocket.recv()
                    data = json.loads(response)
                    logging.debug(f"Received WebSocket data: {data}")

                    signature = data.get("params", {}).get("result", {}).get("value", {}).get("signature")

                    if signature:
                        logging.info(f"New token transaction detected: {signature}")
                        tx_data = fetch_transaction(signature)
                        if tx_data:
                            parsed_data = parse_transaction_data(tx_data)
                            if parsed_data:
                                save_transaction_to_db(parsed_data)
                                notify_discord(parsed_data)
                                logging.info("Transaction processed successfully.")
                            else:
                                logging.warning("No valid data to save.")
                        else:
                            logging.error("Transaction fetch failed.")
        except websockets.ConnectionClosed as e:
            logging.error(f"WebSocket connection closed: {e}. Reconnecting in 5 seconds...")
            await asyncio.sleep(5)
        except Exception as e:
            logging.error(f"Unexpected error: {e}. Reconnecting in 5 seconds...")
            await asyncio.sleep(5)

def main():
    init_db()
    asyncio.run(monitor_tokens())

if __name__ == "__main__":
    main()
