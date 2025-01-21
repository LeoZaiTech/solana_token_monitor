import asyncio
import websockets
import os
from dotenv import load_dotenv
import json
import logging
import sqlite3

# Load environment variables
load_dotenv()
HELIUS_API_KEY = os.getenv("HELIUS_API_KEY")
url = f"wss://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"

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

async def monitor_tokens():
    """Subscribe to token launches and monitor transactions via Helius WebSocket API."""
    while True:
        try:
            async with websockets.connect(url) as websocket:
                logging.info("Subscribed to pump.fun token launches...")

                request = {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "accountSubscribe",
                    "params": [
                        "CM78CPUeXjn8o3yroDHxUtKsZZgoy4GPKPPXfouKNH12",  # Replace with token account
                        {
                            "encoding": "jsonParsed",
                            "commitment": "finalized"
                        }
                    ]
                }

                await websocket.send(json.dumps(request))
                while True:
                    response = await websocket.recv()
                    data = json.loads(response)
                    logging.info(f"Received transaction data: {data}")
                    signature = data.get("params", {}).get("result", {}).get("value", {}).get("signature", None)

                    if signature:
                        logging.info(f"New token transaction detected: {signature}")
                        process_transaction(signature)
        except websockets.ConnectionClosed as e:
            logging.error(f"WebSocket connection closed: {e}. Reconnecting in 5 seconds...")
            await asyncio.sleep(5)
        except asyncio.CancelledError:
            logging.info("WebSocket monitoring cancelled.")
            break
        except Exception as e:
            logging.error(f"Unexpected error: {e}. Reconnecting in 5 seconds...")
            await asyncio.sleep(5)

def main():
    init_db()
    asyncio.run(monitor_tokens())

if __name__ == "__main__":
    main()