import os
import sqlite3
import json
import requests
from datetime import datetime
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv()
HELIUS_API_KEY = os.getenv("HELIUS_API_KEY")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")

# SQLite database initialization
DB_FILE = "solana_transactions.db"

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

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
        logging.error(f"Error fetching transaction: {response.text}")
        return None

def analyze_holders(token_transfers):
    """Analyze token holders and identify potential snipers or insiders."""
    sniper_wallets = set(os.getenv("SNIPER_WALLETS", "").split(","))
    insider_wallets = set(os.getenv("INSIDER_WALLETS", "").split(","))

    holder_count = len(set(tx["destination"] for tx in token_transfers))
    sniper_count = sum(1 for tx in token_transfers if tx["destination"] in sniper_wallets)
    insider_count = sum(1 for tx in token_transfers if tx["destination"] in insider_wallets)

    # Check for high holder concentration
    high_holders = {}
    for tx in token_transfers:
        high_holders[tx["destination"]] = high_holders.get(tx["destination"], 0) + int(tx["amount"])
    
    high_holder_count = sum(1 for amount in high_holders.values() if amount > 0.08 * sum(high_holders.values()))

    return holder_count, sniper_count, insider_count, high_holder_count

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
                # Count buys and sells based on transfers
                if transfer_info.get("destination") == deployer_address:
                    sell_count += 1
                else:
                    buy_count += 1

    buy_sell_ratio = round((buy_count / max(sell_count, 1)) * 100, 2)

    holder_count, sniper_count, insider_count, high_holder_count = analyze_holders(token_transfers)
    token_transfers_json = json.dumps(token_transfers)

    return (signature, block_time, fee, str(error), log_messages, pre_balances, post_balances,
            token_transfers_json, deployer_address, holder_count, sniper_count, insider_count,
            buy_sell_ratio, high_holder_count)

def notify_discord(tx_data):
    """Send a detailed notification to Discord."""
    
    message = (
        f"🚨 **New Token Transaction Alert!**\n\n"
        f"**Transaction Signature:** `{tx_data[0]}`\n"
        f"**Block Time:** {datetime.utcfromtimestamp(tx_data[1]).strftime('%Y-%m-%d %H:%M:%S UTC')}\n"
        f"**Transaction Fee:** {tx_data[2] / 1e9} SOL\n"
        f"**Deployer Address:** `{tx_data[8]}`\n"
        f"**Holder Count:** {tx_data[9]}\n"
        f"**Sniper Count:** {tx_data[10]}\n"
        f"**Insider Count:** {tx_data[11]}\n"
        f"**Buy/Sell Ratio:** {tx_data[12]}%\n"
        f"**High Holder Count:** {tx_data[13]}\n\n"
        f"**Risk Analysis:** "
        f"{'⚠️ High-risk token detected!' if tx_data[9] > 2 or tx_data[10] > 2 or tx_data[12] > 70 or tx_data[13] > 2 else '✅ Token passed criteria'}\n\n"
        f"🔗 [View on Solana Explorer](https://solscan.io/tx/{tx_data[0]})"
    )

    payload = {
        "content": message
    }
    headers = {"Content-Type": "application/json"}
    response = requests.post(DISCORD_WEBHOOK_URL, json=payload, headers=headers)

    if response.status_code == 204:
        logging.info("Notification sent successfully.")
    else:
        logging.error(f"Error sending notification: {response.text}")


def save_transaction_to_db(tx_data):
    """Save parsed transaction data into the SQLite database."""
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''
        INSERT OR IGNORE INTO transactions 
        (signature, block_time, fee, error, log_messages, pre_balances, post_balances, 
         token_transfers, deployer_address, holder_count, sniper_count, insider_count,
         buy_sell_ratio, high_holder_count) 
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', tx_data)
    conn.commit()
    conn.close()

def notify_discord(message):
    """Send a notification to Discord."""
    payload = {
        "content": message
    }
    headers = {"Content-Type": "application/json"}
    response = requests.post(DISCORD_WEBHOOK_URL, json=payload, headers=headers)

    if response.status_code == 204:
        logging.info("Notification sent successfully.")
    else:
        logging.error(f"Error sending notification: {response.text}")

def check_market_cap(market_cap):
    """Check if token surpasses the required market cap."""
    if market_cap >= 30000:
        return True
    logging.warning(f"Token skipped: Market cap {market_cap} below threshold.")
    return False

def main():
    init_db()

    # Sample transaction signature (replace with actual ones)
    sample_signature = "4JWQMMs63xBM3dGKUF29YZnyp6LMEJJGCACo6YBiU2toTqiUDPP79i35Ynct8f6ppCtnRGG7FM7DxomzmYCtuy6F"

    logging.info(f"Fetching transaction: {sample_signature}")
    tx_data = fetch_transaction(sample_signature)

    if tx_data:
        parsed_data = parse_transaction_data(tx_data)
        if parsed_data:
            save_transaction_to_db(parsed_data)
            logging.info("Transaction saved successfully.")

            # Evaluate the token against criteria
            if parsed_data[9] > 2 or parsed_data[10] > 2 or parsed_data[12] > 70 or parsed_data[13] > 2:
                notify_discord(f"🚨 High-risk token detected: {parsed_data[0]}")
            else:
                notify_discord(f"✅ Token passed criteria: {parsed_data[0]}")
        else:
            logging.warning("No valid data to save.")
    else:
        logging.error("Transaction fetch failed.")

if __name__ == "__main__":
    main()
