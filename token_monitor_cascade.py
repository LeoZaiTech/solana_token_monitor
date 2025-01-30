import os
import sqlite3
import json
import logging
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import asyncio
import aiohttp
from dataclasses import dataclass
import websockets
from discord_webhook import DiscordWebhook
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
HELIUS_API_KEY = os.getenv("HELIUS_API_KEY")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")
DB_FILE = "solana_transactions.db"

# Constants
PUMP_FUN_PROGRAM_ID = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
CREATE_INSTRUCTION_DISCRIMINATOR = "82a2124e4f31"

@dataclass
class TokenMetrics:
    """Token metrics for analysis"""
    address: str
    total_holders: int
    dev_sells: int
    sniper_buys: int
    insider_buys: int
    buy_count: int
    sell_count: int
    large_holders: int
    total_supply: float
    holder_balances: Dict[str, float]
    market_cap: float = 0
    peak_market_cap: float = 0

class PriceAlert:
    """Price alert configuration"""
    def __init__(self, token_address: str, target_price: float, is_above: bool = True):
        self.token_address = token_address
        self.target_price = target_price
        self.is_above = is_above
        self.triggered = False

class TokenMonitorCascade:
    """Unified token monitoring system combining best features from v4-10"""
    
    def __init__(self, db_file: str):
        self.db_file = db_file
        self.ws_url = f"wss://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
        self.price_alerts: Dict[str, List[PriceAlert]] = {}
        self.known_snipers = set()
        self.known_insiders = set()
        self.blacklisted_deployers = set()
        self.init_db()

    def init_db(self):
        """Initialize database with proven schema from v8"""
        conn = sqlite3.connect(self.db_file)
        c = conn.cursor()
        
        # Token table
        c.execute('''
            CREATE TABLE IF NOT EXISTS tokens (
                address TEXT PRIMARY KEY,
                deployer_address TEXT,
                creation_time INTEGER,
                market_cap REAL DEFAULT 0,
                peak_market_cap REAL DEFAULT 0,
                total_holders INTEGER DEFAULT 0,
                last_updated INTEGER
            )
        ''')
        
        # Deployer stats table
        c.execute('''
            CREATE TABLE IF NOT EXISTS deployer_stats (
                address TEXT PRIMARY KEY,
                total_tokens INTEGER DEFAULT 0,
                tokens_above_3m INTEGER DEFAULT 0,
                tokens_above_200k INTEGER DEFAULT 0,
                last_token_time INTEGER,
                is_blacklisted BOOLEAN DEFAULT 0
            )
        ''')
        
        # Trades table
        c.execute('''
            CREATE TABLE IF NOT EXISTS trades (
                signature TEXT PRIMARY KEY,
                token_address TEXT,
                from_address TEXT,
                to_address TEXT,
                amount REAL,
                timestamp INTEGER,
                is_buy BOOLEAN,
                FOREIGN KEY (token_address) REFERENCES tokens(address)
            )
        ''')
        
        conn.commit()
        conn.close()

    async def start_monitoring(self):
        """Main monitoring loop combining WebSocket and analysis"""
        logging.info("Starting Cascade Token Monitor...")
        
        # Start WebSocket subscription (v10)
        ws_task = asyncio.create_task(self.subscribe_to_program())
        
        # Start periodic checks (v8)
        while True:
            try:
                await self.update_existing_tokens()
                await asyncio.sleep(30)
            except Exception as e:
                logging.error(f"Error in monitoring loop: {e}")
                await asyncio.sleep(60)

    async def subscribe_to_program(self):
        """Subscribe to pump.fun program (from v10)"""
        while True:
            try:
                async with websockets.connect(self.ws_url) as websocket:
                    subscribe_msg = {
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "programSubscribe",
                        "params": [
                            PUMP_FUN_PROGRAM_ID,
                            {
                                "encoding": "jsonParsed",
                                "commitment": "confirmed",
                                "filters": [
                                    {
                                        "memcmp": {
                                            "offset": 0,
                                            "bytes": CREATE_INSTRUCTION_DISCRIMINATOR
                                        }
                                    }
                                ]
                            }
                        ]
                    }
                    
                    await websocket.send(json.dumps(subscribe_msg))
                    logging.info(f"Subscribed to pump.fun program: {PUMP_FUN_PROGRAM_ID}")
                    
                    while True:
                        try:
                            response = await websocket.recv()
                            data = json.loads(response)
                            
                            if "params" in data and "result" in data["params"]:
                                tx = data["params"]["result"]["value"]
                                if self.is_token_creation(tx):
                                    await self.handle_new_token(tx)
                                    
                        except json.JSONDecodeError as e:
                            logging.error(f"Error decoding WebSocket message: {e}")
                            continue
                            
            except Exception as e:
                logging.error(f"WebSocket connection error: {e}")
                await asyncio.sleep(5)

    def is_token_creation(self, tx: Dict) -> bool:
        """Check if transaction is a token creation (from v10)"""
        try:
            # Check program ID
            for account in tx["transaction"]["message"]["accountKeys"]:
                if account["pubkey"] == PUMP_FUN_PROGRAM_ID:
                    # Check instruction data
                    if "meta" in tx and "innerInstructions" in tx["meta"]:
                        for ix in tx["meta"]["innerInstructions"]:
                            if "instructions" in ix:
                                for instruction in ix["instructions"]:
                                    if "data" in instruction and CREATE_INSTRUCTION_DISCRIMINATOR in instruction["data"]:
                                        return True
            return False
        except Exception as e:
            logging.error(f"Error checking token creation: {e}")
            return False

    async def handle_new_token(self, tx: Dict):
        """Process new token creation (combining v7 and v8)"""
        try:
            token_address = tx["transaction"]["message"]["accountKeys"][1]
            deployer_address = tx["transaction"]["message"]["accountKeys"][0]
            
            # Save to database
            conn = sqlite3.connect(self.db_file)
            c = conn.cursor()
            
            c.execute('''
                INSERT OR IGNORE INTO tokens 
                (address, deployer_address, creation_time, last_updated) 
                VALUES (?, ?, ?, ?)
            ''', (token_address, deployer_address, int(time.time()), int(time.time())))
            
            conn.commit()
            conn.close()
            
            # Start monitoring this token
            asyncio.create_task(self.monitor_token(token_address))
            logging.info(f"Started monitoring new token: {token_address}")
            
        except Exception as e:
            logging.error(f"Error handling new token: {e}")

    async def monitor_token(self, token_address: str):
        """Monitor individual token (from v8)"""
        while True:
            try:
                # Get market cap
                market_cap = await self.get_market_cap(token_address)
                
                if market_cap >= 30000:  # Only analyze if above 30k
                    metrics = await self.analyze_token(token_address)
                    if metrics:
                        await self.check_price_alerts(token_address, market_cap)
                        
                await asyncio.sleep(30)
                
            except Exception as e:
                logging.error(f"Error monitoring token {token_address}: {e}")
                await asyncio.sleep(60)

    async def get_market_cap(self, token_address: str) -> float:
        """Calculate token market cap"""
        # TODO: Implement proper market cap calculation using bonding curve
        return 0.0

    async def analyze_token(self, token_address: str) -> Optional[TokenMetrics]:
        """Comprehensive token analysis (from v7)"""
        # TODO: Implement full token analysis
        return None

    async def check_price_alerts(self, token_address: str, current_price: float):
        """Check and trigger price alerts (from v9)"""
        if token_address not in self.price_alerts:
            return
            
        for alert in self.price_alerts[token_address]:
            if alert.triggered:
                continue
                
            triggered = False
            if alert.is_above and current_price > alert.target_price:
                triggered = True
            elif not alert.is_above and current_price < alert.target_price:
                triggered = True
                
            if triggered:
                alert.triggered = True
                await self.send_notification(
                    f"🚨 Price Alert for {token_address}!\n" +
                    f"Current Price: ${current_price:.6f}\n" +
                    f"Target {'Above' if alert.is_above else 'Below'}: ${alert.target_price:.6f}"
                )

    async def send_notification(self, message: str):
        """Send Discord notification (from v9)"""
        try:
            webhook = DiscordWebhook(url=DISCORD_WEBHOOK_URL)
            webhook.add_embed({
                "title": "Token Alert",
                "description": message,
                "color": 0x00ff00
            })
            response = webhook.execute()
            if response.status_code != 204:
                logging.error(f"Error sending notification: {response.text}")
        except Exception as e:
            logging.error(f"Error sending notification: {e}")

    async def update_existing_tokens(self):
        """Update existing tokens in database"""
        try:
            conn = sqlite3.connect(self.db_file)
            c = conn.cursor()
            
            # Get tokens that need updating (not updated in last 5 minutes)
            c.execute('''
                SELECT address 
                FROM tokens 
                WHERE last_updated < ? 
                OR last_updated IS NULL
            ''', (int(time.time()) - 300,))
            
            tokens = c.fetchall()
            conn.close()
            
            for (token_address,) in tokens:
                try:
                    # Get current market cap
                    market_cap = await self.get_market_cap(token_address)
                    
                    if market_cap >= 30000:  # Only analyze if above 30k
                        metrics = await self.analyze_token(token_address)
                        if metrics:
                            await self.check_price_alerts(token_address, market_cap)
                    
                    # Update last checked time
                    conn = sqlite3.connect(self.db_file)
                    c = conn.cursor()
                    c.execute('''
                        UPDATE tokens 
                        SET last_updated = ? 
                        WHERE address = ?
                    ''', (int(time.time()), token_address))
                    conn.commit()
                    conn.close()
                    
                except Exception as e:
                    logging.error(f"Error updating token {token_address}: {e}")
                    continue
                    
        except Exception as e:
            logging.error(f"Error in update_existing_tokens: {e}")

async def main():
    """Main entry point"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('cascade_monitor.log')
        ]
    )
    
    monitor = TokenMonitorCascade(DB_FILE)
    try:
        await monitor.start_monitoring()
    except KeyboardInterrupt:
        logging.info("Shutting down monitor...")
    except Exception as e:
        logging.error(f"Fatal error: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())
