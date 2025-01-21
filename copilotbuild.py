import os
import sqlite3
import json
import requests
import time
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv
from discord_webhook import DiscordWebhook
import asyncio
from ratelimit import limits, sleep_and_retry

# Constants
MARKET_CAP_THRESHOLD = 30000
WHALE_THRESHOLD = 0.08
MAX_SNIPERS = 2
MAX_INSIDERS = 2
CALLS_PER_SECOND = 3
ONE_SECOND = 1

# Load environment variables
load_dotenv()
HELIUS_API_KEY = os.getenv("HELIUS_API_KEY")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")
JUPITER_API_URL = "https://price.jup.ag/v4/price"

# SQLite database initialization
DB_FILE = "solana_tokens.db"

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def init_db():
    """Initialize database with enhanced schema"""
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    
    # Tokens table
    c.execute('''
    CREATE TABLE IF NOT EXISTS tokens (
        address TEXT PRIMARY KEY,
        deployer TEXT,
        market_cap REAL,
        holder_count INTEGER,
        sniper_count INTEGER,
        insider_count INTEGER,
        whale_count INTEGER,
        first_seen INTEGER,
        last_updated INTEGER
    )''')
    
    # Holders table
    c.execute('''
    CREATE TABLE IF NOT EXISTS holders (
        token_address TEXT,
        holder_address TEXT,
        balance REAL,
        percentage REAL,
        is_sniper BOOLEAN,
        is_insider BOOLEAN,
        last_updated INTEGER,
        PRIMARY KEY (token_address, holder_address)
    )''')
    
    conn.commit()
    conn.close()

@sleep_and_retry
@limits(calls=CALLS_PER_SECOND, period=ONE_SECOND)
async def get_token_price(token_address):
    """Get token price from Jupiter API"""
    try:
        params = {"ids": [token_address]}
        response = requests.get(JUPITER_API_URL, params=params)
        if response.status_code == 200:
            data = response.json()
            return float(data.get("data", {}).get(token_address, {}).get("price", 0))
        return 0
    except Exception as e:
        logging.error(f"Error fetching price: {e}")
        return 0

@sleep_and_retry
@limits(calls=CALLS_PER_SECOND, period=ONE_SECOND)
async def get_token_holders(token_address):
    """Get token holders using Helius API"""
    url = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
    payload = {
        "jsonrpc": "2.0",
        "id": "1",
        "method": "getTokenLargestAccounts",
        "params": [token_address]
    }
    
    try:
        response = requests.post(url, json=payload)
        if response.status_code == 200:
            data = response.json()
            holders = []
            total_supply = 0
            
            for account in data.get("result", {}).get("value", []):
                amount = float(account["amount"])
                total_supply += amount
                holders.append({
                    "address": account["address"],
                    "amount": amount
                })
            
            # Calculate percentages
            for holder in holders:
                holder["percentage"] = (holder["amount"] / total_supply) if total_supply > 0 else 0
                
            return holders
        return []
    except Exception as e:
        logging.error(f"Error fetching holders: {e}")
        return []

def is_sniper(address):
    """Check if address is known sniper"""
    sniper_list = os.getenv("SNIPER_WALLETS", "").split(",")
    return address in sniper_list

def is_insider(address):
    """Check if address is known insider"""
    insider_list = os.getenv("INSIDER_WALLETS", "").split(",")
    return address in insider_list

async def analyze_token(token_address, deployer_address):
    """Analyze token and its holders"""
    # Get market cap
    price = await get_token_price(token_address)
    holders = await get_token_holders(token_address)
    
    if not holders:
        return None
        
    total_supply = sum(h["amount"] for h in holders)
    market_cap = price * total_supply
    
    # Skip if below threshold
    if market_cap < MARKET_CAP_THRESHOLD:
        return None
        
    analysis = {
        "address": token_address,
        "deployer": deployer_address,
        "market_cap": market_cap,
        "holder_count": len(holders),
        "sniper_count": sum(1 for h in holders if is_sniper(h["address"])),
        "insider_count": sum(1 for h in holders if is_insider(h["address"])),
        "whale_count": sum(1 for h in holders if h["percentage"] > WHALE_THRESHOLD)
    }
    
    # Save analysis
    save_analysis(analysis)
    
    return analysis

def save_analysis(analysis):
    """Save token analysis to database"""
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    
    c.execute('''
    INSERT OR REPLACE INTO tokens 
    (address, deployer, market_cap, holder_count, sniper_count, 
     insider_count, whale_count, last_updated)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        analysis["address"],
        analysis["deployer"],
        analysis["market_cap"],
        analysis["holder_count"],
        analysis["sniper_count"],
        analysis["insider_count"],
        analysis["whale_count"],
        int(time.time())
    ))
    
    conn.commit()
    conn.close()

async def monitor_tokens():
    """Monitor tokens with enhanced error handling"""
    while True:
        try:
            # Your existing WebSocket monitoring code
            pass
        except Exception as e:
            logging.error(f"Monitoring error: {e}")
            await asyncio.sleep(5)

def main():
    init_db()
    asyncio.run(monitor_tokens())

if __name__ == "__main__":
    main()