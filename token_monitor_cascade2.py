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
                last_updated INTEGER,
                name TEXT,
                symbol TEXT,
                decimals INTEGER,
                total_supply REAL
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
        logging.info("Starting WebSocket subscription...")
        while True:
            try:
                async with websockets.connect(self.ws_url) as websocket:
                    # First subscribe without filters to see all program transactions
                    subscribe_msg = {
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "programSubscribe",
                        "params": [
                            PUMP_FUN_PROGRAM_ID,
                            {
                                "encoding": "jsonParsed",
                                "commitment": "confirmed"
                            }
                        ]
                    }
                    
                    logging.debug(f"Sending subscription message: {json.dumps(subscribe_msg, indent=2)}")
                    await websocket.send(json.dumps(subscribe_msg))
                    logging.info(f"Subscribed to pump.fun program: {PUMP_FUN_PROGRAM_ID}")
                    
                    # Get subscription confirmation
                    response = await websocket.recv()
                    logging.debug(f"Subscription response: {response}")
                    
                    # Start heartbeat task
                    last_msg_time = time.time()
                    
                    while True:
                        try:
                            response = await asyncio.wait_for(websocket.recv(), timeout=30)
                            last_msg_time = time.time()
                            
                            logging.debug(f"Received WebSocket message: {response[:200]}...")
                            
                            # Log heartbeat every 30 seconds if no transactions
                            if time.time() - last_msg_time > 30:
                                logging.info("Connection alive - No new transactions in last 30s")
                            
                            data = json.loads(response)
                            
                            # Log all message types we receive
                            if "method" in data:
                                logging.debug(f"Message method: {data['method']}")
                            
                            if "params" in data:
                                if "result" in data["params"]:
                                    notification = data["params"]["result"]["value"]
                                    logging.info(f"Received account notification for: {notification['pubkey']}")
                                    
                                    # Log full account data for debugging
                                    logging.debug(f"Account data: {json.dumps(notification['account'], indent=2)}")
                                    
                                    try:
                                        # Process the account update
                                        if "data" in notification["account"]:
                                            data_str = notification["account"]["data"]
                                            if isinstance(data_str, list):
                                                # Base58 encoded data
                                                data_str = data_str[0]
                                            
                                            # Check if this is a token creation
                                            if CREATE_INSTRUCTION_DISCRIMINATOR in data_str:
                                                logging.info(f"Found potential token creation in account: {notification['pubkey']}")
                                                await self.handle_new_token({
                                                    "address": notification["pubkey"],
                                                    "lamports": notification["account"]["lamports"],
                                                    "data": data_str
                                                })
                                    except Exception as e:
                                        logging.error(f"Error processing account data: {e}")
                                        continue
                                else:
                                    logging.debug(f"Received non-result notification: {response[:200]}")
                                    
                        except asyncio.TimeoutError:
                            logging.warning("No messages received in 30s, checking connection...")
                            # Send ping to check connection
                            try:
                                pong_waiter = await websocket.ping()
                                await asyncio.wait_for(pong_waiter, timeout=10)
                                logging.debug("Ping successful - connection alive")
                            except:
                                logging.error("Ping failed - reconnecting...")
                                break
                        except json.JSONDecodeError as e:
                            logging.error(f"Error decoding WebSocket message: {e}")
                            logging.debug(f"Problematic message: {response[:200]}...")
                            continue
                        except Exception as e:
                            logging.error(f"Error processing message: {e}")
                            logging.debug(f"Problematic message: {response[:200]}...")
                            continue
                            
            except Exception as e:
                logging.error(f"WebSocket connection error: {e}")
                logging.info("Attempting to reconnect in 5 seconds...")
                await asyncio.sleep(5)

    async def handle_new_token(self, token_data: Dict):
        """Process new token creation with enhanced validation"""
        try:
            token_address = token_data["address"]
            logging.info(f"Processing potential new token: {token_address}")
            
            # Validate token using Helius API
            async with aiohttp.ClientSession() as session:
                url = f"https://api.helius.xyz/v0/token-metadata?api-key={HELIUS_API_KEY}"
                async with session.post(url, json={"mintAccounts": [token_address]}) as response:
                    if response.status != 200:
                        logging.error(f"Error validating token: {await response.text()}")
                        return
                        
                    data = await response.json()
                    if not data or not isinstance(data, list) or not data[0]:
                        logging.warning(f"Invalid token data for {token_address}")
                        return
                    
                    token_info = data[0]
                    
                    # Extract token metadata
                    token_name = token_info.get("onChainMetadata", {}).get("metadata", {}).get("data", {}).get("name", "Unknown")
                    token_symbol = token_info.get("onChainMetadata", {}).get("metadata", {}).get("data", {}).get("symbol", "")
                    decimals = token_info.get("mint", {}).get("decimals", 0)
                    supply = token_info.get("mint", {}).get("supply", "0")
                    
                    # Get deployer information
                    deployer = token_info.get("mint", {}).get("freezeAuthority", "")
                    if not deployer:
                        deployer = token_info.get("mint", {}).get("mintAuthority", "")
                    
                    # Save to database with enhanced information
                    conn = sqlite3.connect(self.db_file)
                    c = conn.cursor()
                    
                    # First check if token already exists
                    c.execute('SELECT address FROM tokens WHERE address = ?', (token_address,))
                    if c.fetchone() is None:
                        c.execute('''
                            INSERT INTO tokens 
                            (address, deployer_address, creation_time, last_updated, name, symbol, decimals, total_supply) 
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            token_address,
                            deployer,
                            int(time.time()),
                            int(time.time()),
                            token_name,
                            token_symbol,
                            decimals,
                            supply
                        ))
                        
                        # Update deployer stats
                        if deployer:
                            c.execute('''
                                INSERT OR REPLACE INTO deployer_stats 
                                (address, total_tokens, last_token_time) 
                                VALUES (
                                    ?,
                                    COALESCE((SELECT total_tokens + 1 FROM deployer_stats WHERE address = ?), 1),
                                    ?
                                )
                            ''', (deployer, deployer, int(time.time())))
                        
                        conn.commit()
                        
                        # Send notification for new token
                        await self.send_notification(
                            f"🆕 New Token Detected!\n" +
                            f"Name: {token_name}\n" +
                            f"Symbol: {token_symbol}\n" +
                            f"Address: {token_address}\n" +
                            f"Deployer: {deployer}\n" +
                            f"Supply: {float(supply):,.0f}"
                        )
                        
                        logging.info(f"Started monitoring new token: {token_name} ({token_address})")
                        
                        # Start monitoring this token
                        asyncio.create_task(self.monitor_token(token_address))
                    
                    conn.close()
            
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
        """Calculate token market cap using bonding curve and current liquidity"""
        try:
            # Fetch token data from Helius API
            async with aiohttp.ClientSession() as session:
                url = f"https://api.helius.xyz/v0/tokens?api-key={HELIUS_API_KEY}"
                async with session.post(url, json={"mints": [token_address]}) as response:
                    if response.status != 200:
                        logging.error(f"Error fetching token data: {await response.text()}")
                        return 0.0
                    
                    data = await response.json()
                    if not data or not isinstance(data, list) or not data[0]:
                        return 0.0
                    
                    token_data = data[0]
                    
                    # Calculate market cap based on supply and price
                    supply = float(token_data.get("supply", 0))
                    price_usd = float(token_data.get("priceUsd", 0))
                    
                    market_cap = supply * price_usd
                    logging.debug(f"Calculated market cap for {token_address}: ${market_cap:,.2f}")
                    
                    return market_cap
                    
        except Exception as e:
            logging.error(f"Error calculating market cap: {e}")
            return 0.0

    async def analyze_token(self, token_address: str) -> Optional[TokenMetrics]:
        """Comprehensive token analysis including holder analysis and trading patterns"""
        try:
            # Initialize metrics
            metrics = TokenMetrics(
                address=token_address,
                total_holders=0,
                dev_sells=0,
                sniper_buys=0,
                insider_buys=0,
                buy_count=0,
                sell_count=0,
                large_holders=0,
                total_supply=0,
                holder_balances={},
                market_cap=0
            )
            
            # Fetch token data and holder information
            async with aiohttp.ClientSession() as session:
                # Get token metadata
                url = f"https://api.helius.xyz/v0/token-metadata?api-key={HELIUS_API_KEY}"
                async with session.post(url, json={"mintAccounts": [token_address]}) as response:
                    if response.status != 200:
                        logging.error(f"Error fetching token metadata: {await response.text()}")
                        return None
                        
                    data = await response.json()
                    if not data or not isinstance(data, list) or not data[0]:
                        return None
                    
                    token_data = data[0]
                    
                    # Update basic metrics
                    metrics.total_supply = float(token_data.get("mint", {}).get("supply", 0))
                    metrics.market_cap = await self.get_market_cap(token_address)
                    
                    # Get holder information
                    url = f"https://api.helius.xyz/v0/addresses/{token_address}/holders?api-key={HELIUS_API_KEY}"
                    async with session.get(url) as response:
                        if response.status == 200:
                            holders_data = await response.json()
                            
                            # Process holder data
                            for holder in holders_data.get("holders", []):
                                amount = float(holder.get("amount", 0))
                                owner = holder.get("owner")
                                
                                if owner and amount > 0:
                                    metrics.holder_balances[owner] = amount
                                    
                                    # Check if this is a large holder (>1% of supply)
                                    if amount > (metrics.total_supply * 0.01):
                                        metrics.large_holders += 1
                            
                            metrics.total_holders = len(metrics.holder_balances)
                    
                    # Get recent transactions
                    url = f"https://api.helius.xyz/v0/addresses/{token_address}/transactions?api-key={HELIUS_API_KEY}"
                    async with session.get(url) as response:
                        if response.status == 200:
                            tx_data = await response.json()
                            
                            # Analyze trading patterns
                            for tx in tx_data:
                                if "tokenTransfers" in tx:
                                    for transfer in tx["tokenTransfers"]:
                                        if transfer.get("mint") == token_address:
                                            from_addr = transfer.get("fromUserAccount")
                                            to_addr = transfer.get("toUserAccount")
                                            
                                            # Count buys and sells
                                            if from_addr in self.known_insiders:
                                                metrics.insider_buys += 1
                                            if to_addr in self.known_snipers:
                                                metrics.sniper_buys += 1
                                                
                                            if "buy" in tx.get("type", "").lower():
                                                metrics.buy_count += 1
                                            elif "sell" in tx.get("type", "").lower():
                                                metrics.sell_count += 1
            
            logging.info(f"Completed analysis for {token_address}: {metrics}")
            return metrics
            
        except Exception as e:
            logging.error(f"Error analyzing token: {e}")
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
    # Configure more detailed logging
    logging.basicConfig(
        level=logging.DEBUG,  # Changed to DEBUG level
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
