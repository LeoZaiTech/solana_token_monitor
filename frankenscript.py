import os
import sys
import sqlite3
import json
import requests
import time
import threading
import itertools
import logging
import asyncio
import aiohttp
import websockets
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from discord_webhook import DiscordWebhook
from dotenv import load_dotenv
from rate_limiter import registry as rate_limiter_registry

# Load environment variables
load_dotenv()
HELIUS_API_KEY = os.getenv("HELIUS_API_KEY")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")
DB_FILE = "solana_transactions.db"

# Constants
PUMP_FUN_PROGRAM_ID = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
CREATE_INSTRUCTION_DISCRIMINATOR = "82a2124e4f31"

class Spinner:
    def __init__(self, message="Processing"):
        self.spinner = itertools.cycle(['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'])
        self.message = message
        self.running = False
        self.spinner_thread = None

    def spin(self):
        while self.running:
            sys.stdout.write(f'\r{next(self.spinner)} {self.message} ')
            sys.stdout.flush()
            time.sleep(0.1)
        sys.stdout.write('\r✓ Done!\n')
        sys.stdout.flush()

    def __enter__(self):
        self.running = True
        self.spinner_thread = threading.Thread(target=self.spin)
        self.spinner_thread.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.running = False
        if self.spinner_thread:
            self.spinner_thread.join()

@dataclass
class TokenMetrics:
    """Token metrics for analysis"""
    address: str
    total_holders: int = 0
    dev_sells: int = 0
    sniper_buys: int = 0
    insider_buys: int = 0
    buy_count: int = 0
    sell_count: int = 0
    large_holders: int = 0
    total_supply: float = 0
    holder_balances: Dict[str, float] = None
    market_cap: float = 0
    peak_market_cap: float = 0
    confidence_score: float = 0

    def __post_init__(self):
        if self.holder_balances is None:
            self.holder_balances = {}

class TokenMonitor:
    def __init__(self, db_file: str):
        self.db_file = db_file
        self.ws_url = f"wss://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
        self.known_snipers = set()
        self.known_insiders = set()
        self.blacklisted_deployers = set()
        self.helius_limiter = rate_limiter_registry.get_limiter('helius')
        self.jupiter_limiter = rate_limiter_registry.get_limiter('jupiter')
        self.init_db()

    def init_db(self):
        """Initialize database tables"""
        with Spinner("Initializing database..."):
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
                    total_supply REAL,
                    confidence_score REAL DEFAULT 0
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
                    is_blacklisted BOOLEAN DEFAULT 0,
                    failure_rate REAL DEFAULT 0
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
        """Main monitoring loop"""
        print("\n🚀 Starting Frankenscript Token Monitor...\n")
        print("📊 Features enabled:")
        print("  ✓ pump.fun monitoring")
        print("  ✓ Market cap tracking")
        print("  ✓ Holder analysis")
        print("  ✓ Deployer history")
        print("  ✓ Discord notifications")
        print("\n🔍 Monitoring for new tokens...\n")
        
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
                                "commitment": "confirmed"
                            }
                        ]
                    }
                    
                    await websocket.send(json.dumps(subscribe_msg))
                    print("✓ Connected to pump.fun websocket")
                    
                    while True:
                        try:
                            response = await websocket.recv()
                            data = json.loads(response)
                            
                            if "params" in data and "result" in data["params"]:
                                notification = data["params"]["result"]["value"]
                                
                                if "data" in notification["account"]:
                                    data_str = notification["account"]["data"]
                                    if isinstance(data_str, list):
                                        data_str = data_str[0]
                                    
                                    if CREATE_INSTRUCTION_DISCRIMINATOR in data_str:
                                        token_address = notification["pubkey"]
                                        print(f"\n🔔 New token detected: {token_address}")
                                        await self.process_new_token(token_address)
                                        
                        except Exception as e:
                            logging.error(f"Error processing message: {e}")
                            continue
                            
            except Exception as e:
                logging.error(f"WebSocket error: {e}")
                print("\n⚠️ Connection lost. Reconnecting in 5 seconds...")
                await asyncio.sleep(5)

    async def process_new_token(self, token_address: str):
        """Process a new token detection"""
        try:
            with Spinner("Analyzing token..."):
                metrics = await self.analyze_token(token_address)
                
            if metrics and metrics.market_cap >= 30000:
                with Spinner("Running comprehensive analysis..."):
                    # Analyze deployer history
                    deployer_stats = await self.analyze_deployer_history(metrics.deployer_address)
                    
                    # Check if token passes criteria
                    if self.passes_criteria(metrics, deployer_stats):
                        await self.send_discord_notification(metrics, deployer_stats)
                        print(f"✅ Token {token_address} passed all checks! Discord notification sent.")
                    else:
                        print(f"❌ Token {token_address} failed criteria checks.")
            else:
                print(f"ℹ️ Token {token_address} market cap below 30k threshold.")
                
        except Exception as e:
            logging.error(f"Error processing token {token_address}: {e}")

    def passes_criteria(self, metrics: TokenMetrics, deployer_stats: dict) -> bool:
        """Check if token passes all criteria"""
        # Deployer checks
        if deployer_stats['failure_rate'] > 0.97:  # 97% of tokens below 200k
            return False
            
        # Holder and transaction checks
        if (metrics.sniper_buys > 2 or
            metrics.insider_buys > 2 or
            (metrics.buy_count / (metrics.buy_count + metrics.sell_count) if metrics.buy_count + metrics.sell_count > 0 else 0) > 0.7):
            return False
            
        # Large holder check
        large_holder_count = sum(1 for balance in metrics.holder_balances.values() 
                               if balance / metrics.total_supply > 0.08)  # 8% threshold
        if large_holder_count > 2:
            return False
            
        return True

    async def analyze_token(self, token_address: str) -> Optional[TokenMetrics]:
        """Comprehensive token analysis"""
        try:
            metrics = TokenMetrics(address=token_address)
            
            # Get token metadata and market cap
            async with aiohttp.ClientSession() as session:
                url = f"https://api.helius.xyz/v0/token-metadata?api-key={HELIUS_API_KEY}"
                async with session.post(url, json={"mintAccounts": [token_address]}) as response:
                    if response.status == 200:
                        data = await response.json()
                        if data and isinstance(data, list) and data[0]:
                            token_data = data[0]
                            metrics.total_supply = float(token_data.get("mint", {}).get("supply", 0))
                            
                            # Calculate market cap
                            price = await self._get_token_price(token_address)
                            metrics.market_cap = metrics.total_supply * price
                            
                            # Get holder information
                            await self._analyze_holders(metrics)
                            
                            return metrics
            
            return None
            
        except Exception as e:
            logging.error(f"Error analyzing token: {e}")
            return None

    async def _analyze_holders(self, metrics: TokenMetrics):
        """Analyze token holders"""
        try:
            async with aiohttp.ClientSession() as session:
                url = f"https://api.helius.xyz/v0/addresses/{metrics.address}/holders?api-key={HELIUS_API_KEY}"
                async with session.get(url) as response:
                    if response.status == 200:
                        data = await response.json()
                        
                        for holder in data.get("holders", []):
                            address = holder.get("owner")
                            balance = float(holder.get("amount", 0))
                            metrics.holder_balances[address] = balance
                            
                            if self._is_sniper(address):
                                metrics.sniper_buys += 1
                            if self._is_insider(address):
                                metrics.insider_buys += 1
                        
                        metrics.total_holders = len(metrics.holder_balances)
                        
        except Exception as e:
            logging.error(f"Error analyzing holders: {e}")

    async def analyze_deployer_history(self, deployer_address: str) -> dict:
        """Analyze deployer's token history"""
        try:
            conn = sqlite3.connect(self.db_file)
            c = conn.cursor()
            
            c.execute('''
                SELECT COUNT(*) as total,
                       SUM(CASE WHEN market_cap > 3000000 THEN 1 ELSE 0 END) as above_3m,
                       SUM(CASE WHEN market_cap < 200000 THEN 1 ELSE 0 END) as below_200k
                FROM tokens
                WHERE deployer_address = ?
            ''', (deployer_address,))
            
            row = c.fetchone()
            total_tokens = row[0]
            tokens_above_3m = row[1] or 0
            tokens_below_200k = row[2] or 0
            
            failure_rate = tokens_below_200k / total_tokens if total_tokens > 0 else 0
            
            stats = {
                'total_tokens': total_tokens,
                'tokens_above_3m': tokens_above_3m,
                'tokens_below_200k': tokens_below_200k,
                'failure_rate': failure_rate
            }
            
            conn.close()
            return stats
            
        except Exception as e:
            logging.error(f"Error analyzing deployer history: {e}")
            return {'total_tokens': 0, 'tokens_above_3m': 0, 'tokens_below_200k': 0, 'failure_rate': 0}

    async def _get_token_price(self, token_address: str) -> float:
        """Get token price from Jupiter API"""
        try:
            async with aiohttp.ClientSession() as session:
                url = f"https://price.jup.ag/v4/price?ids={token_address}"
                async with session.get(url) as response:
                    if response.status == 200:
                        data = await response.json()
                        return float(data.get("data", {}).get(token_address, {}).get("price", 0))
            return 0
        except Exception as e:
            logging.error(f"Error getting token price: {e}")
            return 0

    def _is_sniper(self, address: str) -> bool:
        """Check if address is a known sniper"""
        return address in self.known_snipers

    def _is_insider(self, address: str) -> bool:
        """Check if address is a known insider"""
        return address in self.known_insiders

    async def send_discord_notification(self, metrics: TokenMetrics, deployer_stats: dict):
        """Send detailed Discord notification"""
        try:
            message = (
                f"🚨 **New Promising Token Alert!**\n\n"
                f"**Token Address:** `{metrics.address}`\n"
                f"**Market Cap:** ${metrics.market_cap:,.2f}\n"
                f"**Holder Stats:**\n"
                f"- Total Holders: {metrics.total_holders}\n"
                f"- Sniper Buys: {metrics.sniper_buys}\n"
                f"- Insider Buys: {metrics.insider_buys}\n\n"
                f"**Deployer History:**\n"
                f"- Total Tokens: {deployer_stats['total_tokens']}\n"
                f"- Tokens > 3M: {deployer_stats['tokens_above_3m']}\n"
                f"- Success Rate: {(1 - deployer_stats['failure_rate']) * 100:.1f}%\n\n"
                f"🔗 [View on Solana Explorer](https://solscan.io/token/{metrics.address})"
            )
            
            webhook = DiscordWebhook(url=DISCORD_WEBHOOK_URL)
            webhook.add_embed({
                "title": "Token Analysis Alert",
                "description": message,
                "color": 0x00ff00
            })
            
            response = webhook.execute()
            if response.status_code != 204:
                logging.error(f"Error sending Discord notification: {response.text}")
                
        except Exception as e:
            logging.error(f"Error sending Discord notification: {e}")

async def demo_mode(monitor: TokenMonitor):
    """Demo mode for showing features"""
    print("\n🎬 Starting Demo Mode with Real Tokens...\n")
    
    # Real tokens from pump.fun
    demo_tokens = [
        # Recent pump.fun tokens
        {
            'address': '4dbsBgkPeiVFRf6bXXsJD8QD8XHkh8arKY5vM37Epump',
            'name': 'Token 1',
        },
        {
            'address': '6k5udKrFne2UUosZFSAh1yAsNSXEJdg7maUD4nsipump',
            'name': 'Token 2',
        },
        {
            'address': 'FbaeKUoLAro539nk37kZujv99iESo2d14hzmGDEspump',
            'name': 'Token 3',
        }
    ]
    
    for token in demo_tokens:
        print(f"\n🔍 Analyzing token: {token['address']}")
        
        # Get token metadata and market cap
        with Spinner("Getting token data..."):
            async with aiohttp.ClientSession() as session:
                url = f"https://api.helius.xyz/v0/token-metadata?api-key={HELIUS_API_KEY}"
                async with session.post(url, json={"mintAccounts": [token['address']]}) as response:
                    if response.status == 200:
                        metadata = await response.json()
                        if metadata and metadata[0]:
                            token_data = metadata[0]
                            print(f"  ✓ Name: {token_data.get('name', 'Unknown')}")
                            print(f"  ✓ Symbol: {token_data.get('symbol', 'Unknown')}")
                            print(f"  ✓ Decimals: {token_data.get('decimals', 0)}")
                            
                            # Get deployer from onchain data
                            token['deployer'] = token_data.get('onChainMetadata', {}).get('updateAuthority', 'Unknown')
                            print(f"  ✓ Deployer: {token['deployer']}")
        
        # Get market cap and price
        with Spinner("Calculating market cap..."):
            price = await monitor._get_token_price(token['address'])
            supply = float(token_data.get('mint', {}).get('supply', 0))
            market_cap = price * supply
            print(f"  ✓ Price: ${price:.6f}")
            print(f"  ✓ Supply: {supply:,.0f}")
            print(f"  ✓ Market Cap: ${market_cap:,.2f}")
        
        # Get holder information
        with Spinner("Analyzing holders..."):
            async with aiohttp.ClientSession() as session:
                url = f"https://api.helius.xyz/v0/addresses/{token['address']}/holders?api-key={HELIUS_API_KEY}"
                async with session.get(url) as response:
                    if response.status == 200:
                        holder_data = await response.json()
                        holders = holder_data.get('holders', [])
                        total_holders = len(holders)
                        
                        # Calculate large holders
                        total_supply = sum(float(h.get('amount', 0)) for h in holders)
                        large_holders = sum(1 for h in holders 
                                          if float(h.get('amount', 0)) / total_supply > 0.08)
                        
                        print(f"  ✓ Total Holders: {total_holders}")
                        print(f"  ✓ Large Holders (>8%): {large_holders}")
        
        # Get transaction history
        with Spinner("Analyzing transactions..."):
            async with aiohttp.ClientSession() as session:
                url = f"https://api.helius.xyz/v0/addresses/{token['address']}/transactions?api-key={HELIUS_API_KEY}"
                async with session.get(url) as response:
                    if response.status == 200:
                        tx_data = await response.json()
                        buys = sum(1 for tx in tx_data if tx.get('type') == 'SWAP')
                        sells = len(tx_data) - buys
                        buy_ratio = buys / (buys + sells) if buys + sells > 0 else 0
                        print(f"  ✓ Buy/Sell Ratio: {buy_ratio:.2f}")
        
        # Analyze deployer history
        with Spinner("Checking deployer history..."):
            async with aiohttp.ClientSession() as session:
                url = f"https://api.helius.xyz/v0/addresses/{token['deployer']}/transactions?api-key={HELIUS_API_KEY}"
                async with session.get(url) as response:
                    if response.status == 200:
                        deployer_txs = await response.json()
                        token_creates = sum(1 for tx in deployer_txs 
                                          if tx.get('type') == 'TOKEN_CREATE')
                        print(f"  ✓ Previous Tokens: {token_creates}")
        
        # Make decision based on real data
        fails = []
        if market_cap < 30000:
            fails.append("Market cap below 30k")
        if large_holders > 2:
            fails.append("Too many large holders (>8%)")
        if buy_ratio > 0.7:
            fails.append("Suspicious buy/sell ratio")
        if token_creates > 0 and token_creates < 3:
            fails.append("New deployer (high risk)")
        
        if not fails:
            print("\n✅ Token passed all checks! Sending Discord notification...")
            metrics = TokenMetrics(
                address=token['address'],
                total_holders=total_holders,
                market_cap=market_cap,
                total_supply=supply
            )
            await monitor.send_discord_notification(
                metrics,
                {
                    'total_tokens': token_creates,
                    'tokens_above_3m': 0,  # Need historical data
                    'failure_rate': 0
                }
            )
        else:
            print("\n❌ Token failed checks:")
            for fail in fails:
                print(f"  • {fail}")
        
        await asyncio.sleep(3)  # Pause between tokens

async def main():
    """Main entry point"""
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('frankenscript.log')
        ]
    )
    
    if not HELIUS_API_KEY or not DISCORD_WEBHOOK_URL:
        print("❌ Error: Please set HELIUS_API_KEY and DISCORD_WEBHOOK_URL in your .env file")
        sys.exit(1)
    
    monitor = TokenMonitor(DB_FILE)
    
    if len(sys.argv) > 1 and sys.argv[1] == '--demo':
        await demo_mode(monitor)
        return
    
    try:
        await monitor.start_monitoring()
    except KeyboardInterrupt:
        print("\n👋 Shutting down monitor...")
    except Exception as e:
        logging.error(f"Fatal error: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())
