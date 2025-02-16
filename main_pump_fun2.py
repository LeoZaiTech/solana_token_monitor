import os
import sqlite3
import json
import logging
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
import asyncio
import aiohttp
from dataclasses import dataclass
import websockets
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

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('pump_fun_monitor.log')
    ]
)

# Set debug level for specific modules
logging.getLogger('websockets').setLevel(logging.WARNING)
logging.getLogger('aiohttp').setLevel(logging.WARNING)

def log_token_info(token_address: str, message: str, level: str = 'info', show_full_address: bool = False):
    """Centralized logging for token-related information
    
    Args:
        token_address: Token address or identifier (e.g. 'summary' for summary logs)
        message: The message to log
        level: Logging level ('debug', 'info', 'warning', 'error')
        show_full_address: If True, shows the full token address
    """
    log_func = getattr(logging, level)
    if token_address == 'summary':
        prefix = '[SUMMARY]'
    else:
        prefix = f'[{token_address if show_full_address else token_address[:8] + "..."}]'
    log_func(f"{prefix} {message}")

@dataclass
class TokenMetrics:
    """Comprehensive token metrics for analysis"""
    # Basic Info
    address: str
    name: str = ''
    symbol: str = ''
    deployer_address: str = ''
    creation_time: int = 0
    
    # Market Metrics
    market_cap: float = 0
    peak_market_cap: float = 0
    total_supply: float = 0
    liquidity: float = 0
    
    # Holder Analysis
    total_holders: int = 0
    holder_balances: Dict[str, float] = None  # Address -> Balance
    large_holders: int = 0  # Holders with >8%
    top_holder_win_rate: float = 0  # Success rate of top holders
    top_holder_pnl: float = 0  # 30-day PNL of top holders
    
    # Transaction Analysis
    buy_count: int = 0
    sell_count: int = 0
    buy_sell_ratio: float = 0
    dev_sells: int = 0
    sniper_buys: int = 0
    insider_buys: int = 0
    
    # Deployer History
    deployer_total_tokens: int = 0
    deployer_tokens_above_3m: int = 0
    deployer_tokens_above_200k: int = 0
    deployer_failure_rate: float = 0
    is_deployer_blacklisted: bool = False
    
    # Social Metrics
    twitter_mentions: int = 0
    notable_mentions: int = 0
    twitter_sentiment: float = 0  # -1 to 1
    name_changes: int = 0
    
    # Risk Assessment
    confidence_score: float = 0
    risk_factors: List[str] = None
    
    def __post_init__(self):
        if self.holder_balances is None:
            self.holder_balances = {}
        if self.risk_factors is None:
            self.risk_factors = []
            
    def calculate_buy_sell_ratio(self) -> float:
        total_trades = self.buy_count + self.sell_count
        if total_trades == 0:
            return 0
        return self.buy_count / total_trades
    
    def calculate_confidence_score(self) -> float:
        """Calculate confidence score based on all metrics"""
        score = 100  # Start with perfect score
        
        # Deduct for risk factors
        deductions = {
            'high_sniper_count': -30,
            'high_insider_count': -30,
            'unbalanced_trades': -20,
            'concentrated_holders': -20,
            'dev_sells': -50,
            'deployer_history': -40,
            'negative_sentiment': -15,
            'name_changes': -10
        }
        
        # Apply deductions based on metrics
        if self.sniper_buys > 2:
            score += deductions['high_sniper_count']
        if self.insider_buys > 2:
            score += deductions['high_insider_count']
        if self.calculate_buy_sell_ratio() > 0.7:
            score += deductions['unbalanced_trades']
        if self.large_holders > 2:
            score += deductions['concentrated_holders']
        if self.dev_sells > 0:
            score += deductions['dev_sells']
        if self.deployer_failure_rate >= 97:
            score += deductions['deployer_history']
        if self.twitter_sentiment < 0:
            score += deductions['negative_sentiment']
        if self.name_changes > 2:
            score += deductions['name_changes']
            
        return max(0, min(100, score))  # Ensure score is between 0 and 100

class PriceAlert:
    """Price alert configuration"""
    def __init__(self, token_address: str, target_price: float, is_above: bool = True):
        self.token_address = token_address
        self.target_price = target_price
        self.is_above = is_above
        self.triggered = False

class TokenMonitor:
    def __init__(self, db_file: str):
        self.db_file = db_file

    async def monitor_market_cap(self, token_address: str):
        """Monitor token's market cap"""
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

class TokenMonitorCascade:
    """Unified token monitoring system combining best features from v4-10"""
    
    def __init__(self, db_file: str):
        self.db_file = db_file
        self.ws_url = f"wss://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
        self.price_alerts: Dict[str, List[PriceAlert]] = {}
        self.known_snipers = set()
        self.known_insiders = set()
        self.blacklisted_deployers = set()
        self.token_monitor = TokenMonitor(db_file)  
        self.helius_limiter = rate_limiter_registry.get_limiter('helius')
        self.jupiter_limiter = rate_limiter_registry.get_limiter('jupiter')
        
        # Token tracking
        self.tokens_processed = 0
        self.tokens_analyzed = 0
        self.small_tokens = {}   # $10K - $100K
        self.medium_tokens = {}  # $100K - $1M
        self.large_tokens = {}   # $1M - $3M
        self.huge_tokens = {}    # $3M+
        
        self.init_db()

    def init_db(self):
        """Initialize database with proven schema from v8"""
        conn = sqlite3.connect(self.db_file)
        c = conn.cursor()
        
        # Drop existing tables to reset schema
        c.execute('DROP TABLE IF EXISTS tokens')
        
        # Enhanced token tracking table
        c.execute('''
            CREATE TABLE IF NOT EXISTS tokens (
                address TEXT PRIMARY KEY,
                deployer_address TEXT,
                creation_time INTEGER,
                discovery_time INTEGER,
                initial_market_cap REAL DEFAULT 0,
                current_market_cap REAL DEFAULT 0,
                peak_market_cap REAL DEFAULT 0,
                total_holders INTEGER DEFAULT 0,
                last_updated INTEGER,
                name TEXT,
                symbol TEXT,
                decimals INTEGER,
                total_supply REAL,
                status TEXT DEFAULT 'discovered',
                analysis_count INTEGER DEFAULT 0,
                last_analysis_time INTEGER,
                passed_30k_time INTEGER,
                passed_analysis BOOLEAN DEFAULT 0,
                notable_holder_count INTEGER DEFAULT 0,
                risk_factors TEXT,
                score_components TEXT
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
        
        # Market cap history table
        c.execute('''
            CREATE TABLE IF NOT EXISTS market_cap_history (
                token_address TEXT,
                timestamp INTEGER,
                market_cap REAL,
                PRIMARY KEY (token_address, timestamp),
                FOREIGN KEY (token_address) REFERENCES tokens(address)
            )
        ''')
        
        # Token analysis results table
        c.execute('''
            CREATE TABLE IF NOT EXISTS analysis_results (
                token_address TEXT,
                timestamp INTEGER,
                market_cap REAL,
                total_holders INTEGER,
                buy_sell_ratio REAL,
                risk_factors TEXT,
                notable_holders TEXT,
                analysis_passed BOOLEAN,
                PRIMARY KEY (token_address, timestamp),
                FOREIGN KEY (token_address) REFERENCES tokens(address)
            )
        ''')
        
        # Transactions table for notifications
        c.execute('''
            CREATE TABLE IF NOT EXISTS transactions (
                signature TEXT PRIMARY KEY,
                block_time INTEGER,
                fee INTEGER,
                token_address TEXT,
                from_address TEXT,
                to_address TEXT,
                amount REAL,
                price REAL,
                deployer_address TEXT,
                holder_count INTEGER,
                sniper_count INTEGER,
                insider_count INTEGER,
                buy_sell_ratio REAL,
                high_holder_count INTEGER,
                FOREIGN KEY (token_address) REFERENCES tokens(address)
            )
        ''')
        
        conn.commit()
        conn.close()

    async def start_monitoring(self):
        """Main monitoring loop combining WebSocket and analysis"""
        print("\n🚀 Starting Cascade Token Monitor...\n")
        
        # Verify configuration
        if not HELIUS_API_KEY:
            print("❌ ERROR: HELIUS_API_KEY not set!")
            return
        if not DISCORD_WEBHOOK_URL:
            print("⚠️ WARNING: DISCORD_WEBHOOK_URL not set! Notifications disabled.")
        
        print("📊 Features enabled:")
        print("  ✓ Real-time pump.fun monitoring")
        print("  ✓ Automatic token detection")
        print("  ✓ Market cap tracking")
        print("  ✓ Holder analysis")
        print("  ✓ Deployer history")
        print("  ✓ Sniper/Insider detection")
        print("  ✓ Discord notifications")
        
        print("\n🔧 Configuration:")
        print(f"  • WebSocket: wss://mainnet.helius-rpc.com/?api-key=****{HELIUS_API_KEY[-4:]}")
        print(f"  • Program ID: {PUMP_FUN_PROGRAM_ID}")
        print(f"  • Database: {self.db_file}")
        
        print("\n🔍 Starting monitoring...")
        
        try:
            # Start WebSocket subscription
            ws_task = asyncio.create_task(self.subscribe_to_program())
            
            # Start periodic background updates
            token_count = 0
            while True:
                try:
                    # Update existing tokens
                    updated = await self.update_existing_tokens()
                    token_count += len(updated) if updated else 0
                    
                    # Show stats
                    print(f"\r📈 Stats: {token_count} tokens monitored | " 
                          f"WebSocket: Active | " 
                          f"Last update: {datetime.now().strftime('%H:%M:%S')}", end='')
                    
                    await asyncio.sleep(30)
                except Exception as e:
                    logging.error(f"Error in monitoring loop: {e}")
                    await asyncio.sleep(60)
                    
        except Exception as e:
            print(f"\n❌ Fatal error: {e}")
        finally:
            if ws_task:
                ws_task.cancel()

    async def subscribe_to_program(self):
        """Subscribe to pump.fun program (from v10)"""
        print("\n🔌 Starting WebSocket subscription...")
        
        # Initialize tracking variables
        self.tokens_processed = 0
        self.tokens_analyzed = 0
        last_status_time = 0
        
        # Verify Discord webhook
        if not DISCORD_WEBHOOK_URL:
            print("⚠️ WARNING: Discord webhook URL not set! Notifications will not work!")
        else:
            print("✅ Discord webhook configured")
            
        while True:
            try:
                # Print periodic status update
                current_time = time.time()
                if current_time - last_status_time >= 60:  # Every minute
                    log_token_info('summary', f"Status: {self.tokens_processed} tokens processed, {self.tokens_analyzed} analyzed | WebSocket: Active | Last update: {time.strftime('%H:%M:%S')}")
                    last_status_time = current_time
                
                print("🔄 Connecting to Helius WebSocket...")
                async with websockets.connect(self.ws_url) as websocket:
                    print("✅ WebSocket connected successfully")
                    
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
                    
                    print(f"📡 Subscribing to pump.fun program: {PUMP_FUN_PROGRAM_ID}")
                    
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
                                    print(f"\nReceived notification: {notification}")
                                    
                                    # Try to get market cap for every notification
                                    if 'pubkey' in notification:
                                        token_address = notification['pubkey']
                                        market_cap = await self.get_market_cap(token_address)
                                        print(f"Market cap for {token_address}: ${market_cap:,.2f}")
                                        
                                        if market_cap > 0:
                                            if market_cap >= 3_000_000:
                                                self.huge_tokens[token_address] = market_cap
                                            elif market_cap >= 1_000_000:
                                                self.large_tokens[token_address] = market_cap
                                            elif market_cap >= 100_000:
                                                self.medium_tokens[token_address] = market_cap
                                            elif market_cap >= 10_000:
                                                self.small_tokens[token_address] = market_cap
                                    
                                    self.tokens_processed += 1
                                    
                                    # Show database summary every 10 tokens
                                    if self.tokens_processed % 10 == 0:
                                        print(f"\nProcessed {self.tokens_processed} tokens... ({time.strftime('%H:%M:%S')})")
                                        
                                        # Query database for token stats
                                        conn = sqlite3.connect(self.db_file)
                                        c = conn.cursor()
                                        
                                        # Get counts and examples for each category
                                        print("\n📊 Database Summary:")
                                        
                                        # HUGE tokens ($3M+)
                                        c.execute('SELECT t.address, t.current_market_cap, t.last_updated, t.deployer_address, t.score_components FROM tokens t WHERE t.current_market_cap >= 3000000 ORDER BY t.last_updated DESC LIMIT 3')
                                        huge = c.fetchall()
                                        c.execute('SELECT COUNT(*) FROM tokens WHERE current_market_cap >= 3000000')
                                        huge_count = c.fetchone()[0]
                                        print(f"\n💎 HUGE Tokens (${huge_count:,}):")
                                        for addr, mcap, ts, deployer, score_data in huge:
                                            scores = json.loads(score_data) if score_data else {}
                                            
                                            print(f"  • ${mcap:,.2f} | {addr}")
                                            print(f"    Deployer: {deployer if deployer else 'Unknown'}")
                                            print(f"    Found: {time.strftime('%H:%M:%S', time.localtime(ts))}")
                                            print(f"    Scores: MC:{scores.get('market_cap_score',0)} H:{scores.get('holder_score',0)} ")
                                            print(f"           T:{scores.get('transaction_score',0)} S:{scores.get('sniper_score',0)} ")
                                            print(f"           TW:{scores.get('twitter_score',0)} D:{scores.get('deployer_score',0)}")
                                            
                                        # LARGE tokens ($1M-$3M)
                                        c.execute('SELECT t.address, t.current_market_cap, t.last_updated, t.deployer_address, t.score_components FROM tokens t WHERE t.current_market_cap >= 1000000 AND t.current_market_cap < 3000000 ORDER BY t.last_updated DESC LIMIT 3')
                                        large = c.fetchall()
                                        c.execute('SELECT COUNT(*) FROM tokens WHERE current_market_cap >= 1000000 AND current_market_cap < 3000000')
                                        large_count = c.fetchone()[0]
                                        print(f"\n🚀 LARGE Tokens (${large_count:,}):")
                                        for addr, mcap, ts, deployer, score_data in large:
                                            scores = json.loads(score_data) if score_data else {}
                                            
                                            print(f"  • ${mcap:,.2f} | {addr}")
                                            print(f"    Deployer: {deployer if deployer else 'Unknown'}")
                                            print(f"    Found: {time.strftime('%H:%M:%S', time.localtime(ts))}")
                                            print(f"    Scores: MC:{scores.get('market_cap_score',0)} H:{scores.get('holder_score',0)} ")
                                            print(f"           T:{scores.get('transaction_score',0)} S:{scores.get('sniper_score',0)} ")
                                            print(f"           TW:{scores.get('twitter_score',0)} D:{scores.get('deployer_score',0)}")
                                            
                                        # MEDIUM tokens ($100K-$1M)
                                        c.execute('SELECT t.address, t.current_market_cap, t.last_updated, t.deployer_address, t.score_components FROM tokens t WHERE t.current_market_cap >= 100000 AND t.current_market_cap < 1000000 ORDER BY t.last_updated DESC LIMIT 3')
                                        medium = c.fetchall()
                                        c.execute('SELECT COUNT(*) FROM tokens WHERE current_market_cap >= 100000 AND current_market_cap < 1000000')
                                        medium_count = c.fetchone()[0]
                                        print(f"\n📈 MEDIUM Tokens (${medium_count:,}):")
                                        for addr, mcap, ts, deployer, score_data in medium:
                                            # Get scores from database
                                            c.execute('SELECT score_components FROM tokens WHERE address = ?', (addr,))
                                            score_data = c.fetchone()[0]
                                            scores = json.loads(score_data) if score_data else {}
                                            
                                            print(f"  • ${mcap:,.2f} | {addr}")
                                            print(f"    Deployer: {deployer if deployer else 'Unknown'}")
                                            print(f"    Found: {time.strftime('%H:%M:%S', time.localtime(ts))}")
                                            print(f"    Scores: MC:{scores.get('market_cap_score',0)} H:{scores.get('holder_score',0)} ")
                                            print(f"           T:{scores.get('transaction_score',0)} S:{scores.get('sniper_score',0)} ")
                                            print(f"           TW:{scores.get('twitter_score',0)} D:{scores.get('deployer_score',0)}")
                                            
                                        # SMALL tokens ($10K-$100K)
                                        c.execute('SELECT address, current_market_cap, last_updated, deployer_address FROM tokens WHERE current_market_cap >= 10000 AND current_market_cap < 100000 ORDER BY last_updated DESC LIMIT 3')
                                        small = c.fetchall()
                                        c.execute('SELECT COUNT(*) FROM tokens WHERE current_market_cap >= 10000 AND current_market_cap < 100000')
                                        small_count = c.fetchone()[0]
                                        print(f"\n🌱 SMALL Tokens (${small_count:,}):")
                                        for addr, mcap, ts, deployer in small:
                                            # Get scores from database
                                            c.execute('SELECT score_components FROM tokens WHERE address = ?', (addr,))
                                            score_data = c.fetchone()[0]
                                            scores = json.loads(score_data) if score_data else {}
                                            
                                            print(f"  • ${mcap:,.2f} | {addr}")
                                            print(f"    Deployer: {deployer if deployer else 'Unknown'}")
                                            print(f"    Found: {time.strftime('%H:%M:%S', time.localtime(ts))}")
                                            print(f"    Scores: MC:{scores.get('market_cap_score',0)} H:{scores.get('holder_score',0)} ")
                                            print(f"           T:{scores.get('transaction_score',0)} S:{scores.get('sniper_score',0)} ")
                                            print(f"           TW:{scores.get('twitter_score',0)} D:{scores.get('deployer_score',0)}")
                                            
                                        conn.close()
                                        print("\n" + "="*50)
                                    
                                    # If we hit 1000000 tokens, show summary and ask to continue
                                    if self.tokens_processed >= 1000000:
                                        print("\n" + "="*50)
                                        print("\n📊 TOKEN SUMMARY")
                                        
                                        print("\n💎 HUGE TOKENS ($3M+):")
                                        if self.huge_tokens:
                                            for addr, mcap in sorted(self.huge_tokens.items(), key=lambda x: x[1], reverse=True):
                                                print(f"  • ${mcap:,.2f} | {addr}")
                                        else:
                                            print("  None found")
                                        print(f"Total: {len(self.huge_tokens)}")
                                        
                                        print("\n🚀 LARGE TOKENS ($1M-$3M):")
                                        if self.large_tokens:
                                            for addr, mcap in sorted(self.large_tokens.items(), key=lambda x: x[1], reverse=True):
                                                print(f"  • ${mcap:,.2f} | {addr}")
                                        else:
                                            print("  None found")
                                        print(f"Total: {len(self.large_tokens)}")
                                        
                                        print("\n📈 MEDIUM TOKENS ($100K-$1M):")
                                        if self.medium_tokens:
                                            for addr, mcap in sorted(self.medium_tokens.items(), key=lambda x: x[1], reverse=True):
                                                print(f"  • ${mcap:,.2f} | {addr}")
                                        else:
                                            print("  None found")
                                        print(f"Total: {len(self.medium_tokens)}")
                                        
                                        print("\n🌱 SMALL TOKENS ($10K-$100K):")
                                        if self.small_tokens:
                                            for addr, mcap in sorted(self.small_tokens.items(), key=lambda x: x[1], reverse=True):
                                                print(f"  • ${mcap:,.2f} | {addr}")
                                        else:
                                            print("  None found")
                                        print(f"Total: {len(self.small_tokens)}")
                                        
                                        print("\nScan complete. Continue? (Enter/q)")
                                        if input().strip().lower() == 'q':
                                            print("Stopping...")
                                            os._exit(0)
                                        print("Starting new scan...")
                                        
                                        # Reset counters and token lists
                                        self.tokens_processed = 0
                                        self.huge_tokens.clear()
                                        self.large_tokens.clear()
                                        self.medium_tokens.clear()
                                        self.small_tokens.clear()
                                    
                                    if logging.getLogger().isEnabledFor(logging.DEBUG):
                                        log_token_info(notification['pubkey'], "Account notification received", "debug")
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
                                                # Validate token account
                                                try:
                                                    async with aiohttp.ClientSession() as session:
                                                        async with session.post(
                                                            f"https://api.helius.xyz/v0/token-metadata?api-key={HELIUS_API_KEY}",
                                                            json={"mintAccounts": [notification["pubkey"]]}
                                                        ) as response:
                                                            if response.status == 200:
                                                                metadata = await response.json()
                                                                if metadata and len(metadata) > 0:
                                                                    token_address = notification['pubkey']
                                                                    try:
                                                                        market_cap = await self.get_market_cap(token_address)
                                                                        print(f"💰 Market Cap: ${market_cap:,.2f}")
                                                                        if market_cap > 0:
                                                                            # Calculate scores
                                                                            deployer_score = 0.0  # We can implement this later
                                                                            holder_score = 20.0 if market_cap > 100000 else 10.0
                                                                            transaction_score = 0  # We can implement this later
                                                                            sniper_score = 0.8  # We can implement this later
                                                                            market_cap_score = 70.0 if market_cap > 1000000 else 40.0
                                                                            twitter_score = 50.0  # We can implement this later
                                                                            
                                                                            score_components = {
                                                                                "deployer_score": deployer_score,
                                                                                "holder_score": holder_score,
                                                                                "transaction_score": transaction_score,
                                                                                "sniper_score": sniper_score,
                                                                                "market_cap_score": market_cap_score,
                                                                                "twitter_score": twitter_score
                                                                            }
                                                                            
                                                                            print(f"\n🔔 New Token Detected:")
                                                                            print(f"📍 Address: {token_address}")
                                                                            print(f"💰 Market Cap: ${market_cap:,.2f}")
                                                                            print(f"\n📊 Token Scores:")
                                                                            print(f"  • Market Cap Score: {market_cap_score}")
                                                                            print(f"  • Holder Score: {holder_score}")
                                                                            print(f"  • Transaction Score: {transaction_score}")
                                                                            print(f"  • Sniper Score: {sniper_score}")
                                                                            print(f"  • Twitter Score: {twitter_score}")
                                                                            print(f"  • Deployer Score: {deployer_score}")
                                                                            print("-" * 50)
                                                                            
                                                                            # Get deployer address from notification
                                                                            deployer_address = notification.get('owner', 'Unknown')
                                                                            
                                                                            # Store in database immediately
                                                                            conn = sqlite3.connect(self.db_file)
                                                                            c = conn.cursor()
                                                                            c.execute(
                                                                                'INSERT OR REPLACE INTO tokens (address, current_market_cap, last_updated, deployer_address) VALUES (?, ?, ?, ?)',
                                                                                (token_address, market_cap, int(time.time()), deployer_address)
                                                                            )
                                                                            conn.commit()
                                                                            conn.close()
                                                                    except Exception as e:
                                                                        print(f"Error getting market cap: {e}")
                                                                    
                                                                    await self.handle_new_token({
                                                                        "address": token_address,
                                                                        "signature": notification.get("signature", ""),
                                                                        "fee": notification["account"].get("lamports", 0),
                                                                        "data": data_str
                                                                    })
                                                except Exception as e:
                                                    logging.error(f"Error validating token account: {e}")
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

    async def handle_new_token(self, token_data: dict):
        """Handle new token creation event"""
        try:
            token_address = token_data.get('address')
            if not token_address:
                return
                
            # Check if token already exists
            conn = sqlite3.connect(self.db_file)
            c = conn.cursor()
            
            c.execute('SELECT address FROM tokens WHERE address = ?', (token_address,))
            if c.fetchone():
                conn.close()
                return
            
            # Get initial market cap
            market_cap = await self.get_market_cap(token_address)
            print(f"Token {token_address}: Market Cap = ${market_cap:,.2f}")
            
            # Categorize token by market cap
            if market_cap > 0:
                # Store in memory for summary
                if market_cap >= 3_000_000:
                    self.huge_tokens[token_address] = market_cap
                elif market_cap >= 1_000_000:
                    self.large_tokens[token_address] = market_cap
                elif market_cap >= 100_000:
                    self.medium_tokens[token_address] = market_cap
                elif market_cap >= 10_000:
                    self.small_tokens[token_address] = market_cap
                    
                # Store in database
                conn = sqlite3.connect(self.db_file)
                c = conn.cursor()
                c.execute(
                    'INSERT OR REPLACE INTO tokens (address, current_market_cap, last_updated, deployer_address) VALUES (?, ?, ?, ?)',
                    (token_address, market_cap, int(time.time()), token_data.get('owner', 'Unknown'))
                )
                conn.commit()
                conn.close()
            
            # If we've processed 1000000 tokens, show summary and ask to continue
            if self.tokens_processed >= 1000000:
                print("\n" + "="*50)
                print("\n📊 TOKEN SUMMARY")
                
                print("\n💎 HUGE TOKENS ($3M+):")
                if self.huge_tokens:
                    # Get deployer addresses for huge tokens
                    c.execute('SELECT address, current_market_cap, deployer_address FROM tokens WHERE address IN ({})'.format(','.join(['?']*len(self.huge_tokens))), list(self.huge_tokens.keys()))
                    token_info = c.fetchall()
                    for addr, mcap, deployer in sorted(token_info, key=lambda x: x[1], reverse=True):
                        print(f"  • {addr} = ${mcap:,.2f}")
                        print(f"    Deployer: {deployer if deployer else 'Unknown'}")
                else:
                    print("  None found")
                print(f"Total: {len(self.huge_tokens)}")
                
                print("\n🚀 LARGE TOKENS ($1M-$3M):")
                if self.large_tokens:
                    # Get deployer addresses for large tokens
                    c.execute('SELECT address, current_market_cap, deployer_address FROM tokens WHERE address IN ({})'.format(','.join(['?']*len(self.large_tokens))), list(self.large_tokens.keys()))
                    token_info = c.fetchall()
                    for addr, mcap, deployer in sorted(token_info, key=lambda x: x[1], reverse=True):
                        print(f"  • {addr} = ${mcap:,.2f}")
                        print(f"    Deployer: {deployer if deployer else 'Unknown'}")
                else:
                    print("  None found")
                print(f"Total: {len(self.large_tokens)}")
                
                print("\n📈 MEDIUM TOKENS ($100K-$1M):")
                if self.medium_tokens:
                    # Get deployer addresses for medium tokens
                    c.execute('SELECT address, current_market_cap, deployer_address FROM tokens WHERE address IN ({})'.format(','.join(['?']*len(self.medium_tokens))), list(self.medium_tokens.keys()))
                    token_info = c.fetchall()
                    for addr, mcap, deployer in sorted(token_info, key=lambda x: x[1], reverse=True):
                        print(f"  • {addr} = ${mcap:,.2f}")
                        print(f"    Deployer: {deployer if deployer else 'Unknown'}")
                else:
                    print("  None found")
                print(f"Total: {len(self.medium_tokens)}")
                
                print("\n🌱 SMALL TOKENS ($10K-$100K):")
                if self.small_tokens:
                    for addr, mcap in sorted(self.small_tokens.items(), key=lambda x: x[1], reverse=True):
                        print(f"  • {addr} = ${mcap:,.2f}")
                else:
                    print("  None found")
                print(f"Total: {len(self.small_tokens)}")
                
                print("\nScan complete. Continue? (Enter/q)")
                if input().strip().lower() == 'q':
                    print("Stopping...")
                    os._exit(0)
                print("Starting new scan...")
                
                # Reset counters and token lists
                self.tokens_processed = 0
                self.huge_tokens.clear()
                self.large_tokens.clear()
                self.medium_tokens.clear()
                self.small_tokens.clear()
            
            # Get initial token data
            metadata = await self._get_token_metadata(token_address)
            if metadata:
                current_time = int(time.time())
                
                # Insert into tokens table
                c.execute('''
                    INSERT INTO tokens (
                        address, creation_time, discovery_time, name, symbol,
                        total_supply, status, last_updated, deployer_address
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    token_address,
                    current_time,
                    current_time,
                    metadata.get('name', ''),
                    metadata.get('symbol', ''),
                    float(metadata.get('supply', 0)),
                    'discovered',
                    current_time,
                    metadata.get('deployer', '')
                ))
                
                # Insert into transactions table
                c.execute('''
                    INSERT INTO transactions (
                        signature, block_time, fee, token_address, from_address,
                        to_address, amount, price, deployer_address, holder_count,
                        sniper_count, insider_count, buy_sell_ratio, high_holder_count
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    token_data.get('signature', ''),
                    current_time,
                    token_data.get('fee', 0),
                    token_address,
                    token_data.get('from', ''),
                    token_data.get('to', ''),
                    float(metadata.get('supply', 0)),
                    0.0,  # Initial price
                    metadata.get('deployer', ''),
                    0,  # Initial holder count
                    0,  # Initial sniper count
                    0,  # Initial insider count
                    0.0,  # Initial buy/sell ratio
                    0  # Initial high holder count
                ))
                
                conn.commit()
                if logging.getLogger().isEnabledFor(logging.DEBUG):
                    log_token_info(token_address, f"Saved token: {metadata.get('name')} ({metadata.get('symbol')})", "debug")
            
            conn.close()
            
            # Start market cap monitoring
            asyncio.create_task(self._monitor_token_market_cap(token_address))
            
        except Exception as e:
            logging.error(f"Error handling new token: {e}")

    async def _monitor_token_market_cap(self, token_address: str):
        """Monitor token's market cap with exponential backoff and rate limiting"""
        backoff = 1
        max_backoff = 300  # 5 minutes
        max_attempts = 100  # Stop after ~8 hours if token never reaches threshold
        analyzed = False
        
        try:
            for attempt in range(max_attempts):
                # Get current market cap
                market_cap = await self.get_market_cap(token_address)
                if market_cap >= 30000 and not analyzed:
                    if logging.getLogger().isEnabledFor(logging.DEBUG):
                        log_token_info(token_address, f"Market Cap: ${market_cap:,.2f}", "debug")
                    
                    # Run comprehensive analysis
                    metrics = await self.analyze_token(token_address)
                    if metrics and metrics.confidence_score >= 70:
                        log_token_info(token_address, f"Analysis complete - Score: {metrics.confidence_score}/100 | Market Cap: ${market_cap:,.2f}")
                        
                        # Get transaction data for notification
                        tx_data = await self.get_transaction_data(token_address, metrics)
                        if tx_data:
                            # Send detailed notification with analysis results
                            await self.send_notification(tx_data, metrics)
                            analyzed = True
                            
                            # If confidence score is high, add to qualifying tokens
                            if metrics.confidence_score >= 70:
                                conn = sqlite3.connect(self.db_file)
                                c = conn.cursor()
                                try:
                                    c.execute("""
                                        UPDATE tokens 
                                        SET status = 'qualified',
                                            confidence_score = ?,
                                            last_updated = ?
                                        WHERE address = ?
                                    """, (metrics.confidence_score, int(time.time()), token_address))
                                    conn.commit()
                                finally:
                                    conn.close()
                    
                    if analyzed:
                        logging.info(f"✅ Analysis complete for {token_address}")
                        break
                
                # Exponential backoff
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)
                
        except Exception as e:
            logging.error(f"Error monitoring market cap for {token_address}: {e}")
        finally:
            conn.close()

    async def get_market_cap(self, token_address: str) -> float:
        """Calculate token market cap using bonding curve and current liquidity"""
        try:
            print(f"\n🔍 Checking market cap for token: {token_address}")
            # Get token metadata with rate limiting
            metadata = await self.helius_limiter.execute_with_retry(
                self._get_token_metadata,
                token_address
            )
            if not metadata:
                print(f"❌ No metadata found for token: {token_address}")
                return 0

            # Get price with rate limiting
            price = await self.jupiter_limiter.execute_with_retry(
                self._get_token_price,
                token_address
            )
            if not price:
                return 0

            supply = metadata.get('supply', 0)
            return float(supply) * price

        except Exception as e:
            logging.error(f"Error calculating market cap: {e}")
            return 0

    async def _get_token_metadata(self, token_address: str):
        async with aiohttp.ClientSession() as session:
            url = f"https://api.helius.xyz/v0/token-metadata?api-key={HELIUS_API_KEY}"
            async with session.post(url, json={"mintAccounts": [token_address]}) as response:
                if response.status != 200:
                    logging.error(f"Error fetching token metadata: {await response.text()}")
                    return None
                    
                data = await response.json()
                if not data or not isinstance(data, list) or not data[0]:
                    return None
                    
                return data[0]

    async def _get_token_price(self, token_address: str):
        async with aiohttp.ClientSession() as session:
            url = f"https://api.helius.xyz/v0/tokens?api-key={HELIUS_API_KEY}"
            async with session.post(url, json={"mints": [token_address]}) as response:
                if response.status != 200:
                    logging.error(f"Error fetching token price: {await response.text()}")
                    return 0.0
                    
                data = await response.json()
                if not data or not isinstance(data, list) or not data[0]:
                    return 0.0
                    
                return float(data[0].get('priceUsd', 0))
                
    def get_qualifying_tokens(self):
        """Get tokens that meet bounty criteria"""
        conn = sqlite3.connect(self.db_file)
        c = conn.cursor()
        
        try:
            c.execute('''
                SELECT 
                    t.address,
                    t.name,
                    t.symbol,
                    t.current_market_cap,
                    t.total_holders,
                    t.notable_holder_count,
                    t.passed_30k_time,
                    ar.risk_factors
                FROM tokens t
                LEFT JOIN analysis_results ar ON 
                    t.address = ar.token_address AND 
                    ar.timestamp = (
                        SELECT MAX(timestamp) 
                        FROM analysis_results 
                        WHERE token_address = t.address
                    )
                WHERE 
                    t.current_market_cap >= 30000
                    AND t.status = 'analyzed'
                    AND (ar.risk_factors IS NULL OR ar.risk_factors = '')
                ORDER BY t.current_market_cap DESC
            ''')
            
            tokens = c.fetchall()
            if tokens:
                logging.info(f"\n🏆 Found {len(tokens)} Qualifying Tokens:")
                for token in tokens:
                    addr, name, symbol, mcap, holders, notable, passed_time, risks = token
                    time_since = int(time.time()) - passed_time if passed_time else 0
                    logging.info(
                        f"💰 {name} ({symbol})\n"
                        f"   Market Cap: ${mcap:,.2f}\n"
                        f"   Holders: {holders}\n"
                        f"   Notable Holders: {notable}\n"
                        f"   Time Since 30k: {time_since//3600}h {(time_since%3600)//60}m\n"
                    )
            else:
                logging.info("\n❗ No tokens currently meet bounty criteria")
                
            return tokens
            
        except Exception as e:
            logging.error(f"Error getting qualifying tokens: {e}")
            return None
        finally:
            conn.close()

    async def analyze_token(self, token_address: str) -> Optional[TokenMetrics]:
        """Comprehensive token analysis including holder analysis and trading patterns"""
        try:
            # Get market cap first to potentially skip analysis
            market_cap = await self.get_market_cap(token_address)
            if market_cap < 30000:
                log_token_info(token_address, f"Skipping analysis - market cap ${market_cap:,.2f} below threshold", "debug")
                return None
            
            log_token_info('summary', f"Analyzing token {token_address[:8]}... (Market cap: ${market_cap:,.2f}) at {time.strftime('%H:%M:%S')}")
            self.tokens_analyzed += 1
            
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
                market_cap=0,
                risk_factors=[]
            )
            
            # Load notable accounts and blacklists
            with open('notable_accounts.json', 'r') as f:
                notable_accounts = json.load(f)
            
            async with aiohttp.ClientSession() as session:
                # 1. Get token metadata and market cap
                metrics.market_cap = await self.get_market_cap(token_address)
                if metrics.market_cap < 30000:
                    if logging.getLogger().isEnabledFor(logging.DEBUG):
                        log_token_info(token_address, f"Market Cap ${metrics.market_cap:,.2f} below threshold", "debug")
                    return None
                
                log_token_info(token_address, f"Market Cap: ${metrics.market_cap:,.2f}")
                
                # 2. Get token metadata and deployer info
                url = f"https://api.helius.xyz/v0/token-metadata?api-key={HELIUS_API_KEY}"
                async with session.post(url, json={"mintAccounts": [token_address]}) as response:
                    if response.status != 200:
                        logging.error(f"Error fetching token metadata: {await response.text()}")
                        return None
                        
                    data = await response.json()
                    if not data or not isinstance(data, list) or not data[0]:
                        return None
                    
                    token_data = data[0]
                    metrics.name = token_data.get("name", "")
                    metrics.symbol = token_data.get("symbol", "")
                    metrics.total_supply = float(token_data.get("mint", {}).get("supply", 0))
                    metrics.deployer_address = token_data.get("mint", {}).get("authority", "")
                    
                    if metrics.market_cap >= 30000:
                        log_token_info(token_address, f"Token: {metrics.name} ({metrics.symbol}) | Supply: {metrics.total_supply:,.0f}")
                    
                    # Check deployer history
                    if metrics.deployer_address:
                        deployer_url = f"https://api.helius.xyz/v0/addresses/{metrics.deployer_address}/tokens?api-key={HELIUS_API_KEY}"
                        async with session.get(deployer_url) as resp:
                            if resp.status == 200:
                                deployer_tokens = await resp.json()
                                metrics.deployer_total_tokens = len(deployer_tokens)
                                
                                # Analyze each token's performance
                                for token in deployer_tokens:
                                    token_mc = await self.get_market_cap(token['address'])
                                    if token_mc > 3000000:
                                        metrics.deployer_tokens_above_3m += 1
                                    if token_mc > 200000:
                                        metrics.deployer_tokens_above_200k += 1
                                
                                # Calculate failure rate
                                if metrics.deployer_total_tokens > 0:
                                    metrics.deployer_failure_rate = 1 - (metrics.deployer_tokens_above_200k / metrics.deployer_total_tokens)
                                    
                                if metrics.deployer_failure_rate >= 0.97:
                                    metrics.risk_factors.append("High deployer failure rate")
                
                # 3. Get holder information
                url = f"https://api.helius.xyz/v0/addresses/{token_address}/holders?api-key={HELIUS_API_KEY}"
                async with session.get(url) as response:
                    if response.status == 200:
                        holders_data = await response.json()
                        top_holders = []
                        
                        for holder in holders_data.get("holders", []):
                            amount = float(holder.get("amount", 0))
                            owner = holder.get("owner")
                            
                            if owner and amount > 0:
                                metrics.holder_balances[owner] = amount
                                percentage = (amount / metrics.total_supply) * 100
                                
                                # Check large holders (>8% as per bounty)
                                if percentage > 8:
                                    metrics.large_holders += 1
                                    if metrics.large_holders > 2:
                                        metrics.risk_factors.append("More than 2 wallets hold >8% supply")
                                
                                # Track top 30 holders for analysis
                                if len(top_holders) < 30 and owner != metrics.deployer_address:
                                    top_holders.append(owner)
                                
                                # Check if holder is notable
                                if owner in notable_accounts:
                                    metrics.notable_mentions += 1
                        
                        metrics.total_holders = len(metrics.holder_balances)
                        
                # 4. Get transaction history
                url = f"https://api.helius.xyz/v0/addresses/{token_address}/transactions?api-key={HELIUS_API_KEY}"
                async with session.get(url) as response:
                    if response.status == 200:
                        tx_data = await response.json()
                        
                        for tx in tx_data:
                            if tx.get('type') == 'SWAP':
                                if tx.get('buyerAddress') in self.known_snipers:
                                    metrics.sniper_buys += 1
                                if tx.get('buyerAddress') in self.known_insiders:
                                    metrics.insider_buys += 1
                                if tx.get('sellerAddress') == metrics.deployer_address:
                                    metrics.dev_sells += 1
                                    
                                if tx.get('type') == 'BUY':
                                    metrics.buy_count += 1
                                elif tx.get('type') == 'SELL':
                                    metrics.sell_count += 1
                        
                        # Calculate buy/sell ratio
                        total_trades = metrics.buy_count + metrics.sell_count
                        if total_trades > 0:
                            metrics.buy_sell_ratio = (metrics.buy_count / total_trades) * 100
                            if metrics.buy_sell_ratio > 70:
                                metrics.risk_factors.append("Suspicious buy/sell ratio")
                
                # 5. Get Twitter metrics
                twitter_url = f"https://api.helius.xyz/v0/social-feed?address={token_address}&api-key={HELIUS_API_KEY}"
                async with session.get(twitter_url) as response:
                    if response.status == 200:
                        social_data = await response.json()
                        metrics.twitter_mentions = len(social_data.get('mentions', []))
                        metrics.name_changes = len(social_data.get('nameHistory', []))
                        
                        # Calculate sentiment
                        sentiments = [mention.get('sentiment', 0) for mention in social_data.get('mentions', [])]
                        if sentiments:
                            metrics.twitter_sentiment = sum(sentiments) / len(sentiments)
                
                # Calculate final confidence score
                metrics.calculate_confidence_score()
                
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
                tx_data = self.get_transaction_data(token_address)
                await self.send_notification(tx_data)

    def get_transaction_data(self, token_address: str, metrics: Optional[TokenMetrics] = None) -> Tuple:
        """Get transaction data for notifications"""
        conn = sqlite3.connect(self.db_file)
        c = conn.cursor()
        try:
            # Get latest transaction
            c.execute("""
                SELECT 
                    t.signature,
                    t.block_time,
                    t.fee,
                    t.token_address,
                    t.from_address,
                    t.to_address,
                    t.amount,
                    t.price,
                    t.deployer_address,
                    t.holder_count,
                    t.sniper_count,
                    t.insider_count,
                    t.buy_sell_ratio,
                    t.high_holder_count
                FROM transactions t
                WHERE t.token_address = ?
                ORDER BY t.block_time DESC
                LIMIT 1
            """, (token_address,))
            tx_data = c.fetchone()
            
            if tx_data is None and metrics is not None:
                # If no transaction found but we have metrics, create a new transaction
                current_time = int(time.time())
                tx_data = (
                    '',  # signature
                    current_time,  # block_time
                    0,  # fee
                    token_address,  # token_address
                    '',  # from_address
                    '',  # to_address
                    metrics.total_supply,  # amount
                    0.0,  # price
                    metrics.deployer_address,  # deployer_address
                    metrics.total_holders,  # holder_count
                    metrics.sniper_buys,  # sniper_count
                    metrics.insider_buys,  # insider_count
                    metrics.buy_sell_ratio,  # buy_sell_ratio
                    metrics.large_holders  # high_holder_count
                )
                
                # Save this transaction
                c.execute("""
                    INSERT INTO transactions (
                        signature, block_time, fee, token_address, from_address,
                        to_address, amount, price, deployer_address, holder_count,
                        sniper_count, insider_count, buy_sell_ratio, high_holder_count
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, tx_data)
                conn.commit()
            
            return tx_data or (None,) * 14
        finally:
            conn.close()

    async def send_notification(self, tx_data, metrics: TokenMetrics):
        """Send a detailed notification to Discord with bounty-specific analysis."""
        try:
            # Determine if token passes bounty criteria
            deployer_check = (
                metrics.deployer_tokens_above_3m > 0 or 
                metrics.deployer_failure_rate < 0.97
            )
            holder_check = (
                metrics.sniper_buys <= 2 and
                metrics.insider_buys <= 2 and
                metrics.buy_sell_ratio <= 70 and
                metrics.large_holders <= 2
            )
            
            # Format analysis results
            embed = {
                "title": "🎯 Pump.fun Token Analysis Report",
                "description": (
                    f"**Token Address:** `{metrics.address}`\n\n"
                    f"**💰 Market Cap:** ${metrics.market_cap:,.2f}\n\n"
                    "**🏗️ Deployer Analysis**\n"
                    f"• Previous Tokens > 3M: {metrics.deployer_tokens_above_3m}\n"
                    f"• Failure Rate (< 200k): {metrics.deployer_failure_rate*100:.1f}%\n"
                    f"• Status: {'✅' if deployer_check else '❌'}\n\n"
                    "**👥 Holder Analysis**\n"
                    f"• Total Holders: {metrics.total_holders}\n"
                    f"• Sniper Buys: {metrics.sniper_buys}/2 {'✅' if metrics.sniper_buys <= 2 else '❌'}\n"
                    f"• Insider Buys: {metrics.insider_buys}/2 {'✅' if metrics.insider_buys <= 2 else '❌'}\n"
                    f"• Buy/Sell Ratio: {metrics.buy_sell_ratio:.1f}% {'✅' if metrics.buy_sell_ratio <= 70 else '❌'}\n"
                    f"• Large Holders: {metrics.large_holders}/2 {'✅' if metrics.large_holders <= 2 else '❌'}\n\n"
                    "**🐦 Social Analysis**\n"
                    f"• Twitter Mentions: {metrics.twitter_mentions}\n"
                    f"• Notable Mentions: {metrics.notable_mentions}\n"
                    f"• Name Changes: {metrics.name_changes}\n"
                    f"• Sentiment: {metrics.twitter_sentiment:+.2f}\n\n"
                    "**🏆 Top Holder Performance**\n"
                    f"• Win Rate: {metrics.top_holder_win_rate*100:.1f}%\n"
                    f"• 30d PnL: {metrics.top_holder_pnl:+.2f}%\n\n"
                    f"**📊 Confidence Score: {metrics.confidence_score}/100**\n"
                    f"Risk Factors: {', '.join(metrics.risk_factors) if metrics.risk_factors else 'None identified'}\n\n"
                    f"🔗 [View on Solana Explorer](https://solscan.io/token/{metrics.address})"
                ),
                "color": 0x00ff00 if metrics.confidence_score >= 70 else 0xff0000
            }
            
            webhook = DiscordWebhook(url=DISCORD_WEBHOOK_URL)
            webhook.add_embed(embed)
            
            response = webhook.execute()
            if response.status_code == 204:
                logging.info(f"✅ Analysis report sent for {metrics.address}")
            else:
                logging.error(f"Error sending notification: {response.text}")
                
        except Exception as e:
            logging.error(f"Error sending Discord notification: {e}")
            logging.error(f"Error in send_notification: {e}")

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