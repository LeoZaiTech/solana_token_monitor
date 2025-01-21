import os
import sqlite3
import json
import requests
import time
import logging
from datetime import datetime
from dotenv import load_dotenv
from discord_webhook import DiscordWebhook
import asyncio
from dataclasses import dataclass
from typing import Dict, List, Optional

# Load environment variables
load_dotenv()
HELIUS_API_KEY = os.getenv("HELIUS_API_KEY")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")

# SQLite database initialization
DB_FILE = "solana_transactions.db"

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

@dataclass
class DeployerStats:
    address: str
    total_tokens: int
    tokens_above_3m: int
    tokens_above_200k: int
    last_token_time: int
    is_blacklisted: bool
    last_updated: int
    failure_rate: float

class DeployerAnalyzer:
    def __init__(self, db_file: str):
        self.db_file = db_file
        self._cache: Dict[str, DeployerStats] = {}
        self._init_db()

    def _init_db(self):
        """Initialize database tables"""
        conn = sqlite3.connect(self.db_file)
        try:
            c = conn.cursor()
            
            # Create deployers table if not exists
            c.execute('''
                CREATE TABLE IF NOT EXISTS deployers (
                    address TEXT PRIMARY KEY,
                    total_tokens INTEGER DEFAULT 0,
                    tokens_above_3m INTEGER DEFAULT 0,
                    tokens_above_200k INTEGER DEFAULT 0,
                    last_token_time INTEGER,
                    is_blacklisted BOOLEAN DEFAULT FALSE,
                    last_updated INTEGER,
                    failure_rate REAL DEFAULT 0
                )
            ''')

            # Create tokens table if not exists
            c.execute('''
                CREATE TABLE IF NOT EXISTS tokens (
                    address TEXT PRIMARY KEY,
                    deployer_address TEXT,
                    launch_time INTEGER,
                    current_market_cap REAL DEFAULT 0,
                    peak_market_cap REAL DEFAULT 0,
                    FOREIGN KEY (deployer_address) REFERENCES deployers (address)
                )
            ''')
            
            conn.commit()
        finally:
            conn.close()

    async def analyze_deployer(self, deployer_address: str) -> DeployerStats:
        """Analyze deployer's history and update stats"""
        # Check cache first
        if self._is_cache_valid(deployer_address):
            return self._cache[deployer_address]

        conn = sqlite3.connect(self.db_file)
        try:
            c = conn.cursor()
            
            # Get deployer's token statistics
            c.execute('''
                SELECT 
                    COUNT(*) as total_tokens,
                    SUM(CASE WHEN peak_market_cap >= 3000000 THEN 1 ELSE 0 END) as tokens_above_3m,
                    SUM(CASE WHEN peak_market_cap >= 200000 THEN 1 ELSE 0 END) as tokens_above_200k,
                    MAX(launch_time) as last_token_time
                FROM tokens 
                WHERE deployer_address = ?
            ''', (deployer_address,))
            
            row = c.fetchone()
            current_time = int(datetime.now().timestamp())
            
            if not row or row[0] == 0:
                stats = DeployerStats(
                    address=deployer_address,
                    total_tokens=0,
                    tokens_above_3m=0,
                    tokens_above_200k=0,
                    last_token_time=current_time,
                    is_blacklisted=False,
                    last_updated=current_time,
                    failure_rate=0
                )
            else:
                total_tokens = row[0]
                tokens_above_3m = row[1] or 0
                tokens_above_200k = row[2] or 0
                last_token_time = row[3]
                
                # Calculate failure rate
                failure_rate = ((total_tokens - tokens_above_200k) / total_tokens) * 100 if total_tokens > 0 else 0
                is_blacklisted = failure_rate >= 97
                
                stats = DeployerStats(
                    address=deployer_address,
                    total_tokens=total_tokens,
                    tokens_above_3m=tokens_above_3m,
                    tokens_above_200k=tokens_above_200k,
                    last_token_time=last_token_time,
                    is_blacklisted=is_blacklisted,
                    last_updated=current_time,
                    failure_rate=failure_rate
                )
            
            # Update deployer record
            c.execute('''
                INSERT OR REPLACE INTO deployers 
                (address, total_tokens, tokens_above_3m, tokens_above_200k, 
                 last_token_time, is_blacklisted, last_updated, failure_rate)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                stats.address, stats.total_tokens, stats.tokens_above_3m,
                stats.tokens_above_200k, stats.last_token_time,
                stats.is_blacklisted, stats.last_updated, stats.failure_rate
            ))
            
            conn.commit()
            self._cache[deployer_address] = stats
            return stats
            
        finally:
            conn.close()

    def _is_cache_valid(self, deployer_address: str) -> bool:
        """Check if cached deployer stats are still valid"""
        if deployer_address not in self._cache:
            return False
            
        stats = self._cache[deployer_address]
        current_time = int(datetime.now().timestamp())
        return (current_time - stats.last_updated) < 3600

    async def update_token_mcap(self, token_address: str, current_mcap: float):
        """Update token's market cap and peak market cap"""
        conn = sqlite3.connect(self.db_file)
        try:
            c = conn.cursor()
            
            # Update current market cap and peak if higher
            c.execute('''
                UPDATE tokens 
                SET current_market_cap = ?,
                    peak_market_cap = CASE 
                        WHEN ? > peak_market_cap THEN ?
                        ELSE peak_market_cap 
                    END
                WHERE address = ?
            ''', (current_mcap, current_mcap, current_mcap, token_address))
            
            conn.commit()
            
            # If market cap thresholds were crossed, invalidate deployer cache
            if current_mcap >= 200000 or current_mcap >= 3000000:
                c.execute('SELECT deployer_address FROM tokens WHERE address = ?', (token_address,))
                row = c.fetchone()
                if row and row[0] in self._cache:
                    del self._cache[row[0]]
                    
        finally:
            conn.close()

    async def is_blacklisted(self, deployer_address: str) -> bool:
        """Check if a deployer is blacklisted"""
        stats = await self.analyze_deployer(deployer_address)
        return stats.is_blacklisted

def init_db():
    """Initialize database schema for token monitoring"""
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    
    # Transactions table for token events
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
    
    # Deployers table for tracking token creators
    c.execute('''
        CREATE TABLE IF NOT EXISTS deployers (
            address TEXT PRIMARY KEY,
            total_tokens INTEGER DEFAULT 0,
            successful_tokens INTEGER DEFAULT 0,
            tokens_above_3m INTEGER DEFAULT 0,
            tokens_above_200k INTEGER DEFAULT 0,
            last_updated INTEGER,
            is_blacklisted BOOLEAN DEFAULT FALSE
        )
    ''')
    
    # Wallets table for tracking snipers and insiders
    c.execute('''
        CREATE TABLE IF NOT EXISTS wallets (
            address TEXT PRIMARY KEY,
            wallet_type TEXT,  -- 'sniper', 'insider', 'whale', 'normal'
            success_rate REAL DEFAULT 0,
            total_trades INTEGER DEFAULT 0,
            profitable_trades INTEGER DEFAULT 0,
            last_14d_trades INTEGER DEFAULT 0,
            last_14d_successes INTEGER DEFAULT 0,
            last_updated INTEGER
        )
    ''')
    
    # Tokens table for detailed token tracking
    c.execute('''
        CREATE TABLE IF NOT EXISTS tokens (
            address TEXT PRIMARY KEY,
            name TEXT,
            symbol TEXT,
            deployer_address TEXT,
            launch_time INTEGER,
            current_market_cap REAL DEFAULT 0,
            peak_market_cap REAL DEFAULT 0,
            holder_count INTEGER DEFAULT 0,
            large_holder_count INTEGER DEFAULT 0,  -- holders with >8% supply
            buy_count INTEGER DEFAULT 0,
            sell_count INTEGER DEFAULT 0,
            twitter_handle TEXT,
            twitter_name_changes INTEGER DEFAULT 0,
            sentiment_score REAL DEFAULT 0,
            confidence_score REAL DEFAULT 0,
            is_verified BOOLEAN DEFAULT FALSE,
            last_updated INTEGER,
            FOREIGN KEY (deployer_address) REFERENCES deployers (address)
        )
    ''')
    
    # Top holders table
    c.execute('''
        CREATE TABLE IF NOT EXISTS top_holders (
            token_address TEXT,
            holder_address TEXT,
            balance REAL,
            percentage REAL,
            win_rate_14d REAL DEFAULT 0,
            pnl_30d REAL DEFAULT 0,
            last_updated INTEGER,
            PRIMARY KEY (token_address, holder_address),
            FOREIGN KEY (token_address) REFERENCES tokens (address),
            FOREIGN KEY (holder_address) REFERENCES wallets (address)
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

    return (signature, block_time, fee, str(error), log_messages, pre_balances, post_balances,
            token_transfers_json, deployer_address, holder_count, sniper_count, insider_count,
            buy_sell_ratio, high_holder_count)

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

async def check_market_cap(token_address):
    """Check if token surpasses 30,000 market cap threshold"""
    url = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
    headers = {"Content-Type": "application/json"}
    
    try:
        # Get token supply
        supply_payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getTokenSupply",
            "params": [token_address]
        }
        
        response = requests.post(url, headers=headers, json=supply_payload)
        if response.status_code == 200:
            supply_data = response.json()
            if "result" in supply_data and "value" in supply_data["result"]:
                total_supply = float(supply_data["result"]["value"]["amount"])
                decimals = supply_data["result"]["value"]["decimals"]
                adjusted_supply = total_supply / (10 ** decimals)
                
                # For now, we'll use a simplified check based on supply
                # You can enhance this with actual DEX price data later
                if adjusted_supply > 0:
                    logging.info(f"Token {token_address} supply: {adjusted_supply}")
                    return True
        return False
    except Exception as e:
        logging.error(f"Error checking market cap: {e}")
        return False

async def analyze_deployer_history(deployer_address):
    """Analyze deployer's previous tokens and their performance."""
    analyzer = DeployerAnalyzer(DB_FILE)
    stats = await analyzer.analyze_deployer(deployer_address)
    logging.info(f"Deployer stats: {stats}")

def notify_discord(tx_data):
    """Send a detailed notification to Discord."""
    try:
        message = (
            f"🚨 **New Token Transaction Alert!**\n\n"
            f"**Transaction Signature:** `{tx_data[0]}`\n"
            f"**Block Time:** {datetime.utcfromtimestamp(tx_data[1]).strftime('%Y-%m-%d %H:%M:%S UTC')}\n"
            f"**Transaction Fee:** {tx_data[2] / 1e9:.8f} SOL\n"
            f"**Deployer Address:** `{tx_data[8]}`\n"
            f"**Holder Count:** {tx_data[9]}\n"
            f"**Sniper Count:** {tx_data[10]}\n"
            f"**Insider Count:** {tx_data[11]}\n"
            f"**Buy/Sell Ratio:** {tx_data[12]}%\n"
            f"**High Holder Count:** {tx_data[13]}\n\n"
            f"🔗 [View on Solana Explorer](https://solscan.io/tx/{tx_data[0]})"
        )

        webhook = DiscordWebhook(url=DISCORD_WEBHOOK_URL)
        webhook.add_embed({
            "title": "Transaction",
            "description": message,
            "color": 0x00ff00  # Green color
        })
        
        response = webhook.execute()
        if response.status_code == 204:
            logging.info("Notification sent successfully.")
        else:
            logging.error(f"Error sending notification: {response.text}")
            
    except Exception as e:
        logging.error(f"Error in notify_discord: {e}")

async def get_token_price(token_address):
    """Get token price from DEX"""
    # This is a placeholder - implement actual DEX price fetching
    # You would typically:
    # 1. Query Jupiter/Raydium/Other DEX API for pool info
    # 2. Calculate price from pool reserves
    # For now, returning mock price for testing
    return 0.1

async def get_transaction_details(signature):
    """Get detailed transaction information"""
    url = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
    headers = {"Content-Type": "application/json"}
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getTransaction",
        "params": [
            signature,
            {"encoding": "jsonParsed"}
        ]
    }
    
    try:
        response = requests.post(url, headers=headers, json=payload)
        if response.status_code == 200:
            return response.json().get("result")
        return None
    except Exception:
        return None

def is_successful_token(tx_data):
    """Determine if a token was successful based on transaction data"""
    # This is a simplified check - implement your own success criteria
    try:
        # Check if transaction was successful
        if tx_data["meta"]["err"]:
            return False
            
        # Check if it has significant holder count (from your DB)
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute('SELECT holder_count FROM transactions WHERE signature = ?', 
                 (tx_data["transaction"]["signatures"][0],))
        result = c.fetchone()
        conn.close()
        
        if result and result[0] >= 100:  # Consider tokens with 100+ holders successful
            return True
            
        return False
    except Exception:
        return False

def main():
    init_db()
    sample_signature = "4JWQMMs63xBM3dGKUF29YZnyp6LMEJJGCACo6YBiU2toTqiUDPP79i35Ynct8f6ppCtnRGG7FM7DxomzmYCtuy6F"

    logging.info(f"Fetching transaction: {sample_signature}")
    tx_data = fetch_transaction(sample_signature)

    if tx_data:
        parsed_data = parse_transaction_data(tx_data)
        if parsed_data:
            save_transaction_to_db(parsed_data)

            deployer_address = parsed_data[8]  # Get deployer address from parsed data
            
            # Insert or update token information in the database
            conn = sqlite3.connect(DB_FILE)
            try:
                c = conn.cursor()
                c.execute('''
                    INSERT OR REPLACE INTO tokens 
                    (address, deployer_address, launch_time, current_market_cap, peak_market_cap)
                    VALUES (?, ?, ?, ?, 
                        CASE 
                            WHEN ? > (SELECT peak_market_cap FROM tokens WHERE address = ?)
                            THEN ?
                            ELSE (SELECT peak_market_cap FROM tokens WHERE address = ?)
                        END
                    )
                ''', ('TestToken123', deployer_address, int(datetime.now().timestamp()), 250000, 
                      250000, 'TestToken123', 250000, 'TestToken123'))
                conn.commit()
            except sqlite3.Error as e:
                logging.error(f"Error inserting token data: {e}")
            finally:
                conn.close()

            notify_discord(parsed_data)
            logging.info("Transaction processed successfully.")

            # Analyze the actual deployer from the transaction
            try:
                asyncio.run(analyze_deployer_history(deployer_address))
            except Exception as e:
                logging.error(f"Error analyzing deployer history: {e}")
        else:
            logging.warning("No valid data to save.")
    else:
        logging.error("Transaction fetch failed.")

    # Run market cap check and deployer analysis
    try:
        asyncio.run(check_market_cap("token_address"))
        asyncio.run(analyze_deployer_history(deployer_address))
    except Exception as e:
        logging.error(f"Error running additional analyses: {e}")

if __name__ == "__main__":
    main()

