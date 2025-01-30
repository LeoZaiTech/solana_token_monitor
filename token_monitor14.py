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
from typing import Dict, List, Optional, Tuple
import aiohttp
from collections import defaultdict, Counter

# Load environment variables
load_dotenv()
HELIUS_API_KEY = os.getenv("HELIUS_API_KEY")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")

# SQLite database initialization
DB_FILE = "solana_transactions.db"

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Constants
PUMP_FUN_PROGRAM_ID = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
CREATE_INSTRUCTION_DISCRIMINATOR = "82a2124e4f31"

@dataclass
class TokenMetrics:
    """Class to store token metrics for analysis"""
    address: str
    total_holders: int
    dev_sells: int
    sniper_buys: int
    insider_buys: int
    buy_count: int
    sell_count: int
    large_holders: int  # Holders with >8% supply
    total_supply: float
    holder_balances: Dict[str, float]  # address -> balance

class HolderAnalyzer:
    def __init__(self, db_file: str):
        self.db_file = db_file
        self._known_snipers = set()  # Cache of known sniper wallets
        self._known_insiders = set()  # Cache of known insider wallets
        self._init_db()

    def _init_db(self):
        """Initialize database tables for holder analysis"""
        conn = sqlite3.connect(self.db_file)
        try:
            c = conn.cursor()
            
            # Create holders table
            c.execute('''
                CREATE TABLE IF NOT EXISTS holders (
                    token_address TEXT,
                    holder_address TEXT,
                    balance REAL,
                    last_updated INTEGER,
                    PRIMARY KEY (token_address, holder_address)
                )
            ''')
            
            # Create wallet_labels table
            c.execute('''
                CREATE TABLE IF NOT EXISTS wallet_labels (
                    address TEXT PRIMARY KEY,
                    label TEXT,  -- 'sniper', 'insider', 'developer'
                    confidence REAL,
                    last_updated INTEGER
                )
            ''')
            
            conn.commit()
        except sqlite3.Error as e:
            logging.error(f"Database error: {e}")
        finally:
            conn.close()

    async def analyze_token_metrics(self, token_address: str, deployer_address: str) -> TokenMetrics:
        """Analyze token holder metrics and transaction patterns"""
        try:
            # Get token holder data from Helius API
            holder_data = await self._fetch_holder_data(token_address)
            
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
                holder_balances={}
            )
            
            # Process holder data
            await self._process_holder_data(holder_data, metrics, deployer_address)
            
            # Get transaction history
            tx_history = await self._fetch_transaction_history(token_address)
            
            # Analyze transaction patterns
            await self._analyze_transactions(tx_history, metrics, deployer_address)
            
            return metrics
            
        except Exception as e:
            logging.error(f"Error analyzing token metrics: {e}")
            raise

    async def _fetch_holder_data(self, token_address: str) -> List[Dict]:
        """Fetch token holder data from Helius API"""
        url = f"https://api.helius.xyz/v0/token/{token_address}/holders"
        headers = {"Authorization": f"Bearer {HELIUS_API_KEY}"}
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers) as response:
                    if response.status == 200:
                        return await response.json()
                    else:
                        logging.error(f"Error fetching holder data: {response.status}")
                        return []
        except Exception as e:
            logging.error(f"Error in _fetch_holder_data: {e}")
            return []

    async def _process_holder_data(self, holder_data: List[Dict], metrics: TokenMetrics, deployer_address: str):
        """Process holder data and update metrics"""
        try:
            total_supply = sum(float(h['balance']) for h in holder_data)
            metrics.total_supply = total_supply
            
            for holder in holder_data:
                balance = float(holder['balance'])
                address = holder['address']
                
                # Update holder balances
                metrics.holder_balances[address] = balance
                
                # Check for large holders (>8% supply)
                if balance / total_supply > 0.08 and address != deployer_address:
                    metrics.large_holders += 1
                
                # Update total holder count
                if balance > 0:
                    metrics.total_holders += 1
                    
            # Save holder data to database
            self._save_holder_data(metrics.address, metrics.holder_balances)
            
        except Exception as e:
            logging.error(f"Error processing holder data: {e}")

    async def _analyze_transactions(self, tx_history: List[Dict], metrics: TokenMetrics, deployer_address: str):
        """Analyze transaction patterns"""
        try:
            for tx in tx_history:
                if self._is_sell_transaction(tx):
                    metrics.sell_count += 1
                    if tx['from'] == deployer_address:
                        metrics.dev_sells += 1
                else:
                    metrics.buy_count += 1
                    
                    # Check for sniper/insider buys
                    buyer = tx['to']
                    if await self._is_sniper_wallet(buyer):
                        metrics.sniper_buys += 1
                    if await self._is_insider_wallet(buyer):
                        metrics.insider_buys += 1
                        
        except Exception as e:
            logging.error(f"Error analyzing transactions: {e}")

    async def _is_sniper_wallet(self, address: str) -> bool:
        """Check if wallet is a known sniper"""
        if address in self._known_snipers:
            return True
            
        conn = sqlite3.connect(self.db_file)
        try:
            c = conn.cursor()
            c.execute('SELECT label FROM wallet_labels WHERE address = ? AND label = "sniper"', (address,))
            result = c.fetchone()
            if result:
                self._known_snipers.add(address)
                return True
        finally:
            conn.close()
        return False

    async def _is_insider_wallet(self, address: str) -> bool:
        """Check if wallet is a known insider"""
        if address in self._known_insiders:
            return True
            
        conn = sqlite3.connect(self.db_file)
        try:
            c = conn.cursor()
            c.execute('SELECT label FROM wallet_labels WHERE address = ? AND label = "insider"', (address,))
            result = c.fetchone()
            if result:
                self._known_insiders.add(address)
                return True
        finally:
            conn.close()
        return False

    def _save_holder_data(self, token_address: str, holder_balances: Dict[str, float]):
        """Save holder data to database"""
        conn = sqlite3.connect(self.db_file)
        try:
            c = conn.cursor()
            current_time = int(time.time())
            
            for address, balance in holder_balances.items():
                c.execute('''
                    INSERT INTO holders (token_address, holder_address, balance, last_updated)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(token_address, holder_address) DO UPDATE SET
                        balance = excluded.balance,
                        last_updated = excluded.last_updated
                ''', (token_address, address, balance, current_time))
                
            conn.commit()
        except sqlite3.Error as e:
            logging.error(f"Database error in _save_holder_data: {e}")
        finally:
            conn.close()

    def is_token_safe(self, metrics: TokenMetrics) -> Tuple[bool, Dict[str, float], str]:
        """
        Check if token meets safety criteria and return detailed safety metrics
        Returns: (is_safe, safety_scores, reason_string)
        """
        reasons = []
        safety_scores = {}
        
        # Calculate sniper safety score (0-100)
        sniper_score = 100
        if metrics.sniper_buys > 0:
            sniper_penalty = min(metrics.sniper_buys * 25, 80)
            sniper_score = max(0, sniper_score - sniper_penalty)
            if metrics.sniper_buys > 2:
                reasons.append(f"Too many sniper buys ({metrics.sniper_buys})")
        safety_scores['sniper'] = sniper_score
            
        # Calculate insider safety score (0-100)
        insider_score = 100
        if metrics.insider_buys > 0:
            insider_penalty = min(metrics.insider_buys * 25, 80)
            insider_score = max(0, insider_score - insider_penalty)
            if metrics.insider_buys > 2:
                reasons.append(f"Too many insider buys ({metrics.insider_buys})")
        safety_scores['insider'] = insider_score
            
        # Calculate trading pattern score (0-100)
        trading_score = 100
        total_tx = metrics.buy_count + metrics.sell_count
        if total_tx > 0:
            buy_ratio = metrics.buy_count / total_tx * 100
            if buy_ratio > 70:
                ratio_penalty = min((buy_ratio - 70) * 2, 80)
                trading_score = max(0, trading_score - ratio_penalty)
                reasons.append(f"Suspicious buy ratio ({buy_ratio:.1f}%)")
        safety_scores['trading'] = trading_score
                
        # Calculate holder concentration score (0-100)
        holder_score = 100
        if metrics.large_holders > 0:
            holder_penalty = min(metrics.large_holders * 30, 90)
            holder_score = max(0, holder_score - holder_penalty)
            if metrics.large_holders > 2:
                reasons.append(f"Too many large holders ({metrics.large_holders})")
        safety_scores['holder'] = holder_score
            
        # Calculate developer activity score (0-100)
        dev_score = 100
        if metrics.dev_sells > 0:
            dev_penalty = min(metrics.dev_sells * 40, 100)
            dev_score = max(0, dev_score - dev_penalty)
            reasons.append(f"Developer has sold tokens ({metrics.dev_sells} times)")
        safety_scores['developer'] = dev_score
            
        # Token is considered safe if all scores are above threshold
        is_safe = all(score >= 50 for score in safety_scores.values())
            
        return is_safe, safety_scores, ", ".join(reasons)

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
                    score_components TEXT,
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

class TwitterAnalyzer:
    """Analyzes Twitter data for token monitoring using Twitter API v2"""
    def __init__(self, bearer_token=None):
        self.bearer_token = bearer_token or os.getenv('TWITTER_BEARER_TOKEN')
        self.api_base_url = "https://api.twitter.com/2"
        self.session = None
        
    async def _ensure_session(self):
        if not self.session:
            self.session = aiohttp.ClientSession()
            
    async def analyze_sentiment(self, token_address: str) -> Dict:
        """Analyze Twitter sentiment and notable mentions for a token"""
        try:
            tweets = await self._get_recent_tweets(token_address)
            if not tweets:
                return {
                    'sentiment': 'neutral',
                    'score': 0.5,
                    'notable_mentions': 0,
                    'tweet_count': 0
                }
                
            # Count notable mentions (accounts with >10k followers)
            notable_mentions = sum(1 for tweet in tweets 
                                 if tweet.get('author', {}).get('followers_count', 0) > 10000)
                                 
            # Basic sentiment analysis
            positive_words = {'moon', 'gem', 'bullish', 'buy', 'good', 'great', 'profit'}
            negative_words = {'scam', 'rug', 'dump', 'sell', 'bad', 'avoid', 'fake'}
            
            total_score = 0
            for tweet in tweets:
                text = tweet.get('text', '').lower()
                pos_count = sum(1 for word in positive_words if word in text)
                neg_count = sum(1 for word in negative_words if word in text)
                tweet_score = (pos_count - neg_count) / (pos_count + neg_count + 1)
                total_score += tweet_score
                
            avg_score = total_score / len(tweets) if tweets else 0.5
            sentiment = 'positive' if avg_score > 0.6 else 'negative' if avg_score < 0.4 else 'neutral'
            
            return {
                'sentiment': sentiment,
                'score': avg_score,
                'notable_mentions': notable_mentions,
                'tweet_count': len(tweets)
            }
            
        except Exception as e:
            logging.error(f"Error analyzing sentiment: {e}")
            return {
                'sentiment': 'neutral',
                'score': 0.5,
                'notable_mentions': 0,
                'tweet_count': 0
            }
            
    async def _get_recent_tweets(self, token_address: str) -> List[Dict]:
        """Get recent tweets mentioning the token address"""
        if not self.bearer_token:
            logging.warning("TWITTER_BEARER_TOKEN not set. Twitter analysis will be disabled.")
            return []
            
        await self._ensure_session()
        query = f"{token_address} -is:retweet"
        
        url = f"{self.api_base_url}/tweets/search/recent"
        params = {
            'query': query,
            'max_results': 100,
            'tweet.fields': 'created_at,public_metrics',
            'user.fields': 'public_metrics,created_at'
        }
        
        try:
            async with self.session.get(url, params=params, 
                                      headers={'Authorization': f'Bearer {self.bearer_token}'}) as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get('data', [])
        except Exception as e:
            logging.error(f"Error fetching tweets: {e}")
            return []

class HolderAnalyzer:
    """Analyzes token holders and transactions"""
    def __init__(self, helius_api_key: str):
        self.helius_api_key = helius_api_key
        self.api_url = "https://mainnet.helius-rpc.com/?api-key=" + helius_api_key
        
    async def analyze_holders(self, token_address: str, deployer_address: str) -> Dict:
        """
        Comprehensive holder and transaction analysis:
        - Developer token sales
        - Sniper/insider detection
        - Buy/sell ratio
        - Large holder concentration
        """
        try:
            # Get token holders
            holders = await self._get_token_holders(token_address)
            if not holders:
                return self._get_empty_result()
                
            total_supply = sum(h['amount'] for h in holders)
            
            # Check developer sales
            dev_sales = await self._check_developer_sales(token_address, deployer_address)
            
            # Identify snipers and insiders
            sniper_count = sum(1 for h in holders if await self._is_sniper(h['owner']))
            insider_count = sum(1 for h in holders if await self._is_insider(h['owner']))
            
            # Calculate buy/sell ratio
            buy_sell_ratio = await self._get_buy_sell_ratio(token_address)
            
            # Check large holder concentration
            large_holders = sum(1 for h in holders 
                              if (h['amount'] / total_supply) > 0.08 
                              and h['owner'] != deployer_address)
            
            # Determine if token passes all checks
            is_safe = all([
                sniper_count <= 2,
                insider_count <= 2,
                buy_sell_ratio <= 0.7,
                large_holders <= 2,
                not dev_sales['has_sold']
            ])
            
            return {
                'is_safe': is_safe,
                'metrics': {
                    'total_holders': len(holders),
                    'sniper_count': sniper_count,
                    'insider_count': insider_count,
                    'buy_sell_ratio': buy_sell_ratio,
                    'large_holders': large_holders,
                    'dev_sales': dev_sales
                },
                'failure_reasons': self._get_failure_reasons(
                    sniper_count, insider_count, buy_sell_ratio, 
                    large_holders, dev_sales['has_sold']
                )
            }
            
        except Exception as e:
            logging.error(f"Error analyzing holders: {e}")
            return self._get_empty_result()
            
    async def _get_token_holders(self, token_address: str) -> List[Dict]:
        """Get all token holders using Helius API"""
        payload = {
            "jsonrpc": "2.0",
            "id": "my-id",
            "method": "getTokenLargestAccounts",
            "params": [token_address]
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.post(self.api_url, json=payload) as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get('result', {}).get('value', [])
        return []
        
    async def _check_developer_sales(self, token_address: str, deployer_address: str) -> Dict:
        """Check if developer has sold any tokens"""
        payload = {
            "jsonrpc": "2.0",
            "id": "my-id",
            "method": "getSignaturesForAddress",
            "params": [deployer_address, {"limit": 100}]
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(self.api_url, json=payload) as response:
                    if response.status == 200:
                        data = await response.json()
                        signatures = [tx['signature'] for tx in data.get('result', [])]
                        
                        # Check each transaction for token sales
                        for sig in signatures:
                            tx_data = await self._get_transaction(sig)
                            if self._is_token_sale(tx_data, token_address, deployer_address):
                                return {'has_sold': True, 'first_sale_signature': sig}
                                
        except Exception as e:
            logging.error(f"Error checking developer sales: {e}")
            
        return {'has_sold': False, 'first_sale_signature': None}
        
    async def _is_sniper(self, address: str) -> bool:
        """Check if address is a known sniper"""
        conn = sqlite3.connect(DB_FILE)
        try:
            c = conn.cursor()
            c.execute("SELECT 1 FROM known_snipers WHERE address = ?", (address,))
            return bool(c.fetchone())
        finally:
            conn.close()
            
    async def _is_insider(self, address: str) -> bool:
        """Check if address is a known insider"""
        conn = sqlite3.connect(DB_FILE)
        try:
            c = conn.cursor()
            c.execute("SELECT 1 FROM known_insiders WHERE address = ?", (address,))
            return bool(c.fetchone())
        finally:
            conn.close()
            
    async def _get_buy_sell_ratio(self, token_address: str) -> float:
        """Calculate buy/sell ratio for token"""
        conn = sqlite3.connect(DB_FILE)
        try:
            c = conn.cursor()
            c.execute("""
                SELECT 
                    COUNT(CASE WHEN transaction_type = 'buy' THEN 1 END) as buys,
                    COUNT(CASE WHEN transaction_type = 'sell' THEN 1 END) as sells
                FROM transactions 
                WHERE token_address = ?
            """, (token_address,))
            
            buys, sells = c.fetchone()
            total = buys + sells
            return buys / total if total > 0 else 0
            
        finally:
            conn.close()
            
    def _get_failure_reasons(self, sniper_count: int, insider_count: int, 
                           buy_sell_ratio: float, large_holders: int, 
                           dev_has_sold: bool) -> List[str]:
        """Get list of reasons why token failed checks"""
        reasons = []
        if sniper_count > 2:
            reasons.append("Too many snipers")
        if insider_count > 2:
            reasons.append("Too many insiders")
        if buy_sell_ratio > 0.7:
            reasons.append("Suspicious buy/sell ratio")
        if large_holders > 2:
            reasons.append("Too many large holders")
        if dev_has_sold:
            reasons.append("Developer has sold tokens")
        return reasons
        
    def _get_empty_result(self) -> Dict:
        """Return empty result structure"""
        return {
            'is_safe': False,
            'metrics': {
                'total_holders': 0,
                'sniper_count': 0,
                'insider_count': 0,
                'buy_sell_ratio': 0,
                'large_holders': 0,
                'dev_sales': {'has_sold': False, 'first_sale_signature': None}
            },
            'failure_reasons': ["Could not analyze holders"]
        }
        
    async def _get_transaction(self, signature: str) -> Dict:
        """Get transaction details from Helius"""
        payload = {
            "jsonrpc": "2.0",
            "id": "my-id",
            "method": "getTransaction",
            "params": [signature]
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.post(self.api_url, json=payload) as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get('result', {})
        return {}

class TopHolderAnalyzer:
    """Analyzes performance of top token holders"""
    
    def __init__(self, helius_api_key: str):
        self.helius_api_key = helius_api_key
        self.api_url = "https://mainnet.helius-rpc.com/?api-key=" + helius_api_key
        
    async def analyze_top_holders(self, token_address: str, deployer_address: str, liquidity_address: str) -> Dict:
        """
        Analyze top 30 holders' performance:
        - 14-day win rate (tokens reaching 1M from <100k)
        - 30-day PNL
        """
        try:
            # Get top holders excluding deployer and liquidity
            holders = await self._get_top_holders(token_address, deployer_address, liquidity_address)
            if not holders:
                return self._get_empty_result()
                
            # Calculate metrics for each holder
            holder_metrics = []
            for holder in holders[:30]:  # Top 30 holders
                metrics = await self._calculate_holder_metrics(holder['owner'])
                holder_metrics.append({
                    'address': holder['owner'],
                    'balance': holder['amount'],
                    'win_rate': metrics['win_rate'],
                    'pnl_30d': metrics['pnl_30d']
                })
                
            # Calculate average metrics
            avg_win_rate = sum(h['win_rate'] for h in holder_metrics) / len(holder_metrics)
            avg_pnl = sum(h['pnl_30d'] for h in holder_metrics) / len(holder_metrics)
            
            return {
                'holder_metrics': holder_metrics,
                'summary': {
                    'avg_win_rate': avg_win_rate,
                    'avg_pnl_30d': avg_pnl,
                    'total_analyzed': len(holder_metrics)
                }
            }
            
        except Exception as e:
            logging.error(f"Error analyzing top holders: {e}")
            return self._get_empty_result()
            
    async def _get_top_holders(self, token_address: str, deployer_address: str, liquidity_address: str) -> List[Dict]:
        """Get top token holders excluding deployer and liquidity"""
        payload = {
            "jsonrpc": "2.0",
            "id": "my-id",
            "method": "getTokenLargestAccounts",
            "params": [token_address]
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.post(self.api_url, json=payload) as response:
                if response.status == 200:
                    data = await response.json()
                    holders = data.get('result', {}).get('value', [])
                    
                    # Filter out deployer and liquidity addresses
                    return [h for h in holders 
                           if h['owner'] not in {deployer_address, liquidity_address}]
        return []
        
    async def _calculate_holder_metrics(self, holder_address: str) -> Dict:
        """Calculate win rate and PNL for a holder"""
        two_weeks_ago = int(time.time()) - (14 * 24 * 60 * 60)
        thirty_days_ago = int(time.time()) - (30 * 24 * 60 * 60)
        
        conn = sqlite3.connect(DB_FILE)
        try:
            c = conn.cursor()
            
            # Get 14-day win rate
            c.execute("""
                SELECT COUNT(*) as total_tokens,
                       SUM(CASE WHEN peak_market_cap >= 1000000 THEN 1 ELSE 0 END) as winning_tokens
                FROM transactions t
                JOIN tokens tok ON t.token_address = tok.address
                WHERE t.buyer_address = ?
                AND t.timestamp >= ?
                AND tok.market_cap <= 100000
                GROUP BY t.buyer_address
            """, (holder_address, two_weeks_ago))
            
            row = c.fetchone()
            total_tokens = row[0] if row else 0
            winning_tokens = row[1] if row else 0
            win_rate = winning_tokens / total_tokens if total_tokens > 0 else 0
            
            # Calculate 30-day PNL
            c.execute("""
                SELECT SUM(
                    CASE 
                        WHEN transaction_type = 'buy' THEN -amount * price
                        ELSE amount * price
                    END
                ) as pnl
                FROM transactions
                WHERE (buyer_address = ? OR seller_address = ?)
                AND timestamp >= ?
            """, (holder_address, holder_address, thirty_days_ago))
            
            pnl = c.fetchone()[0] or 0
            
            return {
                'win_rate': win_rate,
                'pnl_30d': pnl
            }
            
        finally:
            conn.close()
            
    def _get_empty_result(self) -> Dict:
        """Return empty result structure"""
        return {
            'holder_metrics': [],
            'summary': {
                'avg_win_rate': 0,
                'avg_pnl_30d': 0,
                'total_analyzed': 0
            }
        }

class TokenScorer:
    def __init__(self, db_file: str):
        self.db_file = db_file
        self.deployer_analyzer = DeployerAnalyzer(db_file)
        self.holder_analyzer = HolderAnalyzer(db_file)
        self.twitter_analyzer = TwitterAnalyzer()  # Add Twitter analyzer

    async def calculate_token_score(self, token_address: str, tx_data: tuple) -> float:
        """Calculate a comprehensive confidence score for a token."""
        try:
            scores = {
                'deployer_score': await self._get_deployer_score(tx_data[8]),  # 25%
                'holder_score': self._get_holder_score(tx_data),  # 20%
                'transaction_score': self._get_transaction_score(tx_data),  # 20%
                'sniper_score': self._get_sniper_score(tx_data),  # 15%
                'market_cap_score': await self._get_market_cap_score(token_address),  # 10%
                'twitter_score': await self._get_twitter_score(token_address)  # 10%
            }

            weights = {
                'deployer_score': 0.25,
                'holder_score': 0.20,
                'transaction_score': 0.20,
                'sniper_score': 0.15,
                'market_cap_score': 0.10,
                'twitter_score': 0.10
            }

            final_score = sum(scores[key] * weights[key] for key in scores)
            await self._save_score(token_address, final_score, scores)
            return float(final_score)
        except Exception as e:
            logging.error(f"Error in calculate_token_score: {str(e)}")
            return 0.0

    async def _get_twitter_score(self, token_address: str) -> float:
        """Calculate score based on Twitter metrics."""
        try:
            twitter_metrics = await self.twitter_analyzer.analyze_sentiment(token_address)
            if not twitter_metrics:
                return 50.0  # Neutral score if no Twitter data

            risk_score = 1 - twitter_metrics['score']
            
            # Convert risk score (0-1 where 1 is high risk) to confidence score (0-100 where 100 is good)
            twitter_score = (1 - risk_score) * 100
            
            return float(twitter_score)
        except Exception as e:
            logging.error(f"Error in _get_twitter_score: {str(e)}")
            return 50.0  # Neutral score on error

    async def _get_deployer_score(self, deployer_address: str) -> float:
        """Calculate score based on deployer's history"""
        stats = await self.deployer_analyzer.analyze_deployer(deployer_address)
        
        if stats.is_blacklisted:
            return 0.0

        # Calculate success rate
        success_rate = (stats.tokens_above_200k / max(stats.total_tokens, 1)) * 100
        
        # Bonus for having tokens above 3M
        high_success_bonus = min((stats.tokens_above_3m / max(stats.total_tokens, 1)) * 20, 20)
        
        # Experience factor (more tokens = more experience, up to a point)
        experience_factor = min(stats.total_tokens / 10, 1.0)  # Cap at 10 tokens
        
        base_score = (success_rate * 0.8) + high_success_bonus
        return min(base_score * (0.5 + 0.5 * experience_factor), 100)

    def _get_holder_score(self, tx_data: tuple) -> float:
        """Calculate score based on holder distribution"""
        try:
            holder_count = tx_data[9] or 0
            high_holder_count = tx_data[13] or 0
            
            # Penalize if too few holders
            if holder_count < 10:
                return 20.0
                
            # Heavily penalize if too many high-balance holders
            if high_holder_count > 2:
                return max(40 - (high_holder_count - 2) * 20, 0)
                
            # Score based on holder count, with diminishing returns
            holder_score = min((holder_count / 100) * 80, 80)
            
            # Bonus for good distribution
            distribution_bonus = 20 if high_holder_count <= 1 else 10
            
            return min(holder_score + distribution_bonus, 100)
        except (IndexError, TypeError, ValueError) as e:
            logging.error(f"Error in holder score calculation: {e}")
            return 0.0

    def _get_transaction_score(self, tx_data: tuple) -> float:
        """Calculate score based on transaction patterns"""
        try:
            buy_sell_ratio = float(tx_data[12] or 0)
            
            # Penalize extreme buy/sell ratios
            if buy_sell_ratio > 70 or buy_sell_ratio < 30:
                return max(50 - abs(50 - buy_sell_ratio), 0)
                
            # Ideal ratio is close to 50/50
            ratio_score = 100 - abs(50 - buy_sell_ratio)
            
            return float(ratio_score)
        except (IndexError, TypeError, ValueError) as e:
            logging.error(f"Error in transaction score calculation: {e}")
            return 0.0

    def _get_sniper_score(self, tx_data: tuple) -> float:
        """Calculate score based on sniper and insider presence using detailed metrics"""
        try:
            # Safely parse integer values with error handling
            def safe_int(val, default=0):
                try:
                    if isinstance(val, str) and '{' in val:  # Check for error JSON
                        return default
                    return int(val) if val is not None else default
                except (ValueError, TypeError):
                    return default

            sniper_count = safe_int(tx_data[10])
            insider_count = safe_int(tx_data[11])
            
            # Get detailed safety metrics
            metrics = TokenMetrics(
                address=str(tx_data[0]),
                total_holders=safe_int(tx_data[3]),
                total_supply=float(safe_int(tx_data[4], 1)),
                holder_balances={},
                dev_sells=safe_int(tx_data[9]),
                sniper_buys=sniper_count,
                insider_buys=insider_count,
                buy_count=safe_int(tx_data[5]),
                sell_count=safe_int(tx_data[6]),
                large_holders=safe_int(tx_data[7])
            )
            
            # Use enhanced safety check
            _, safety_scores, _ = self.holder_analyzer.is_token_safe(metrics)
            
            # Calculate weighted safety score
            weights = {
                'sniper': 0.35,
                'insider': 0.25,
                'trading': 0.20,
                'holder': 0.10,
                'developer': 0.10
            }
            
            final_score = sum(safety_scores[key] * weights[key] for key in weights)
            return float(final_score)
            
        except Exception as e:
            logging.error(f"Error in sniper score calculation: {e}")
            return 0.0

    async def _get_market_cap_score(self, token_address: str) -> float:
        """Calculate score based on market cap growth and stability"""
        try:
            conn = sqlite3.connect(self.db_file)
            c = conn.cursor()
            
            # Get current market cap from tokens table instead
            c.execute('''
                SELECT current_market_cap, peak_market_cap
                FROM tokens 
                WHERE address = ?
            ''', (token_address,))
            
            result = c.fetchone()
            if not result or not result[0]:
                return 50.0  # Default score for new tokens
                
            current_mcap = float(result[0])
            peak_mcap = float(result[1] or current_mcap)
            
            # Score based on market cap and stability
            if current_mcap < 30000:
                return 30.0
            elif current_mcap < 100000:
                return 50.0
            elif current_mcap < 500000:
                return 70.0
            else:
                # Add stability bonus if current is close to peak
                base_score = 80.0
                stability_ratio = current_mcap / peak_mcap
                stability_bonus = stability_ratio * 20.0
                return min(base_score + stability_bonus, 100.0)
                
        except Exception as e:
            logging.error(f"Error in market cap score calculation: {e}")
            return 50.0  # Default score on error
        finally:
            if 'conn' in locals():
                conn.close()

    async def _save_score(self, token_address: str, final_score: float, component_scores: dict):
        """Save the calculated scores to the database"""
        conn = sqlite3.connect(self.db_file)
        try:
            c = conn.cursor()
            c.execute('''
                UPDATE tokens 
                SET confidence_score = ?,
                    score_components = ?,
                    last_updated = ?
                WHERE address = ?
            ''', (
                final_score,
                json.dumps(component_scores),
                int(datetime.now().timestamp()),
                token_address
            ))
            conn.commit()
        finally:
            if 'conn' in locals():
                conn.close()

class VolumeAnalyzer:
    """Analyzes trading volume patterns to detect manipulation."""
    
    def __init__(self, db_file: str):
        self.db_file = db_file
        self._init_db()
        
    def _init_db(self):
        """Initialize database tables for volume analysis."""
        conn = sqlite3.connect(self.db_file)
        try:
            c = conn.cursor()
            
            # Create trades table for detailed trade analysis
            c.execute('''
                CREATE TABLE IF NOT EXISTS token_trades (
                    tx_signature TEXT PRIMARY KEY,
                    token_address TEXT,
                    from_address TEXT,
                    to_address TEXT,
                    amount REAL,
                    timestamp INTEGER,
                    price REAL,
                    FOREIGN KEY (token_address) REFERENCES tokens(address)
                )
            ''')
            
            conn.commit()
        except sqlite3.Error as e:
            logging.error(f"Database error in VolumeAnalyzer: {e}")
        finally:
            conn.close()
            
    async def analyze_volume(self, token_address: str, recent_trades: List[Dict]) -> Dict[str, float]:
        """
        Analyze trading patterns to detect fake volume.
        Returns a dictionary of manipulation scores (0-1, higher is more suspicious).
        """
        try:
            scores = {
                'circular_trading': await self._detect_circular_trading(recent_trades),
                'time_pattern': await self._detect_time_patterns(recent_trades),
                'size_pattern': await self._detect_size_patterns(recent_trades),
                'wash_trading': await self._detect_wash_trading(recent_trades)
            }
            
            # Save trades for historical analysis
            await self._save_trades(token_address, recent_trades)
            
            return scores
            
        except Exception as e:
            logging.error(f"Error in analyze_volume: {e}")
            return {'error': 1.0}
            
    async def _detect_circular_trading(self, trades: List[Dict]) -> float:
        """Detect tokens being passed between related wallets."""
        try:
            trade_graph = defaultdict(set)
            wallet_groups = []
            
            # Build trade graph
            for trade in trades:
                trade_graph[trade['from_address']].add(trade['to_address'])
                
            # Find circular patterns (groups of wallets trading among themselves)
            visited = set()
            
            def find_group(wallet, current_group):
                if wallet in visited:
                    return
                visited.add(wallet)
                current_group.add(wallet)
                for connected in trade_graph[wallet]:
                    find_group(connected, current_group)
            
            for wallet in trade_graph:
                if wallet not in visited:
                    group = set()
                    find_group(wallet, group)
                    if len(group) > 1:
                        wallet_groups.append(group)
            
            # Calculate suspicion score based on group sizes and trade frequency
            if not wallet_groups:
                return 0.0
                
            max_group_size = max(len(group) for group in wallet_groups)
            avg_group_size = sum(len(group) for group in wallet_groups) / len(wallet_groups)
            
            # More suspicious if there are larger groups of wallets trading among themselves
            return min(1.0, (avg_group_size / 3) * (max_group_size / 5))
            
        except Exception as e:
            logging.error(f"Error in _detect_circular_trading: {e}")
            return 1.0
            
    async def _detect_time_patterns(self, trades: List[Dict]) -> float:
        """Detect suspicious regularity in trade timing."""
        try:
            if len(trades) < 3:
                return 0.0
                
            # Calculate time differences between consecutive trades
            time_diffs = []
            for i in range(1, len(trades)):
                diff = trades[i]['timestamp'] - trades[i-1]['timestamp']
                time_diffs.append(diff)
            
            # Check for suspicious patterns in time differences
            avg_diff = sum(time_diffs) / len(time_diffs)
            variance = sum((d - avg_diff) ** 2 for d in time_diffs) / len(time_diffs)
            std_dev = variance ** 0.5
            
            # More suspicious if time differences are too regular (low standard deviation)
            regularity_score = 1.0 - min(1.0, (std_dev / avg_diff))
            
            return regularity_score
            
        except Exception as e:
            logging.error(f"Error in _detect_time_patterns: {e}")
            return 1.0
            
    async def _detect_size_patterns(self, trades: List[Dict]) -> float:
        """Detect suspicious patterns in trade sizes."""
        try:
            if len(trades) < 3:
                return 0.0
                
            # Count occurrences of each trade size
            size_counts = Counter(trade['amount'] for trade in trades)
            
            # Calculate what percentage of trades are duplicates
            total_trades = len(trades)
            duplicate_trades = sum(count - 1 for count in size_counts.values() if count > 1)
            
            return min(1.0, duplicate_trades / total_trades)
            
        except Exception as e:
            logging.error(f"Error in _detect_size_patterns: {e}")
            return 1.0
            
    async def _detect_wash_trading(self, trades: List[Dict]) -> float:
        """Detect same wallet trading with itself through different routes."""
        try:
            wallet_volume = defaultdict(float)
            wallet_trades = defaultdict(int)
            
            for trade in trades:
                wallet_volume[trade['from_address']] += trade['amount']
                wallet_volume[trade['to_address']] += trade['amount']
                wallet_trades[trade['from_address']] += 1
                wallet_trades[trade['to_address']] += 1
            
            # Calculate concentration of volume in top wallets
            total_volume = sum(wallet_volume.values())
            if total_volume == 0:
                return 0.0
                
            top_wallets = sorted(wallet_volume.items(), key=lambda x: x[1], reverse=True)[:3]
            top_volume = sum(vol for _, vol in top_wallets)
            
            # More suspicious if few wallets account for most volume
            concentration_score = top_volume / total_volume
            
            return concentration_score
            
        except Exception as e:
            logging.error(f"Error in _detect_wash_trading: {e}")
            return 1.0
            
    async def _save_trades(self, token_address: str, trades: List[Dict]):
        """Save trade data for historical analysis."""
        conn = sqlite3.connect(self.db_file)
        try:
            c = conn.cursor()
            for trade in trades:
                c.execute('''
                    INSERT OR REPLACE INTO token_trades 
                    (tx_signature, token_address, from_address, to_address, amount, timestamp, price)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (
                    trade['signature'],
                    token_address,
                    trade['from_address'],
                    trade['to_address'],
                    trade['amount'],
                    trade['timestamp'],
                    trade.get('price', 0)
                ))
            conn.commit()
        except sqlite3.Error as e:
            logging.error(f"Database error in _save_trades: {e}")
        finally:
            conn.close()

    def is_volume_suspicious(self, scores: Dict[str, float]) -> Tuple[bool, float]:
        """
        Determine if the volume patterns are suspicious.
        Returns (is_suspicious, confidence_score).
        """
        if 'error' in scores:
            return True, 1.0
            
        # Weight the different types of manipulation
        weights = {
            'circular_trading': 0.3,
            'time_pattern': 0.2,
            'size_pattern': 0.2,
            'wash_trading': 0.3
        }
        
        # Calculate weighted average
        weighted_score = sum(scores[k] * weights[k] for k in weights)
        
        # Consider volume suspicious if weighted score is above 0.6
        return weighted_score > 0.6, weighted_score

class TokenSafetyChecker:
    def __init__(self, db_file: str):
        self.db_file = db_file
        self.volume_analyzer = VolumeAnalyzer(db_file)
        
    async def check_token_safety(self, token_address: str, metrics: TokenMetrics) -> Dict[str, float]:
        """Comprehensive token safety check including volume analysis"""
        scores = {
            'holder_score': self._check_holder_distribution(metrics),
            'transaction_score': self._check_transaction_patterns(metrics),
            'sniper_score': self._check_sniper_presence(metrics),
            'insider_score': self._check_insider_presence(metrics)
        }
        
        # Get recent trades for volume analysis
        recent_trades = await self._get_recent_trades(token_address)
        volume_scores = await self.volume_analyzer.analyze_volume(token_address, recent_trades)
        
        # Combine volume analysis with other scores
        is_suspicious, volume_score = self.volume_analyzer.is_volume_suspicious(volume_scores)
        scores['volume_score'] = 1.0 - volume_score  # Invert so higher is better
        
        # Add detailed volume metrics
        scores.update({
            f"volume_{k}": 1.0 - v  # Invert so higher is better
            for k, v in volume_scores.items()
            if k != 'error'
        })
        
        return scores
        
    async def _get_recent_trades(self, token_address: str) -> List[Dict]:
        """Fetch recent trades for the token from our database."""
        conn = sqlite3.connect(self.db_file)
        try:
            c = conn.cursor()
            c.execute('''
                SELECT tx_signature, from_address, to_address, amount, timestamp, price
                FROM token_trades
                WHERE token_address = ?
                ORDER BY timestamp DESC
                LIMIT 100
            ''', (token_address,))
            
            trades = []
            for row in c.fetchall():
                trades.append({
                    'signature': row[0],
                    'from_address': row[1],
                    'to_address': row[2],
                    'amount': row[3],
                    'timestamp': row[4],
                    'price': row[5]
                })
            return trades
            
        except sqlite3.Error as e:
            logging.error(f"Database error in _get_recent_trades: {e}")
            return []
        finally:
            conn.close()

    def _check_holder_distribution(self, metrics: TokenMetrics) -> float:
        """Check if holder distribution is suspicious"""
        # Implement your logic here
        return 1.0

    def _check_transaction_patterns(self, metrics: TokenMetrics) -> float:
        """Check if transaction patterns are suspicious"""
        # Implement your logic here
        return 1.0

    def _check_sniper_presence(self, metrics: TokenMetrics) -> float:
        """Check if sniper presence is suspicious"""
        # Implement your logic here
        return 1.0

    def _check_insider_presence(self, metrics: TokenMetrics) -> float:
        """Check if insider presence is suspicious"""
        # Implement your logic here
        return 1.0

class TokenHeuristics:
    """Advanced heuristics for detecting suspicious token behavior"""
    
    def __init__(self, helius_api_key: str):
        self.helius_api_key = helius_api_key
        self.api_url = "https://mainnet.helius-rpc.com/?api-key=" + helius_api_key
        
    async def analyze_token_safety(self, token_address: str, deployer_address: str, liquidity_address: str) -> Dict:
        """
        Run comprehensive token safety checks:
        - Top 10 holder concentration
        - Volume manipulation detection
        - Supply allocation analysis
        """
        try:
            # Get all holders and their balances
            holders = await self._get_token_holders(token_address)
            if not holders:
                return self._get_empty_result()
                
            total_supply = sum(h['amount'] for h in holders)
            
            # Check top 10 holder concentration
            top_10_concentration = await self._check_holder_concentration(
                holders, total_supply, deployer_address, liquidity_address)
                
            # Check for volume manipulation
            volume_analysis = await self._analyze_volume(token_address)
            
            # Check supply allocation
            supply_analysis = await self._analyze_supply_allocation(
                holders, total_supply, deployer_address)
                
            # Determine if token passes all checks
            is_safe = all([
                not top_10_concentration['is_suspicious'],
                not volume_analysis['is_suspicious'],
                not supply_analysis['is_suspicious']
            ])
            
            return {
                'is_safe': is_safe,
                'metrics': {
                    'top_10_concentration': top_10_concentration,
                    'volume_analysis': volume_analysis,
                    'supply_analysis': supply_analysis
                },
                'failure_reasons': self._get_failure_reasons(
                    top_10_concentration, volume_analysis, supply_analysis)
            }
            
        except Exception as e:
            logging.error(f"Error in token heuristics: {e}")
            return self._get_empty_result()
            
    async def _check_holder_concentration(self, holders: List[Dict], 
                                        total_supply: float,
                                        deployer_address: str,
                                        liquidity_address: str) -> Dict:
        """Check if top 10 non-liquidity holders own >25% of supply"""
        # Filter out deployer, liquidity, and burn addresses
        filtered_holders = [
            h for h in holders 
            if h['owner'] not in {deployer_address, liquidity_address}
            and not self._is_burn_address(h['owner'])
        ]
        
        # Sort by balance and get top 10
        top_10 = sorted(filtered_holders, 
                       key=lambda x: float(x['amount']), 
                       reverse=True)[:10]
                       
        # Calculate concentration
        concentration = sum(float(h['amount']) for h in top_10) / total_supply
        
        return {
            'is_suspicious': concentration > 0.25,
            'concentration': concentration,
            'details': {
                'top_10_addresses': [h['owner'] for h in top_10],
                'individual_percentages': [
                    float(h['amount']) / total_supply for h in top_10
                ]
            }
        }
        
    async def _analyze_volume(self, token_address: str) -> Dict:
        """Detect suspicious trading volume patterns"""
        # Get recent trades
        trades = await self._get_recent_trades(token_address)
        if not trades:
            return {'is_suspicious': True, 'reason': 'No trading activity'}
            
        # Group trades by time windows (e.g., 5-minute intervals)
        window_size = 300  # 5 minutes
        volume_windows = {}
        
        for trade in trades:
            window = trade['timestamp'] // window_size
            if window not in volume_windows:
                volume_windows[window] = 0
            volume_windows[window] += trade['amount'] * trade['price']
            
        # Calculate volume metrics
        if not volume_windows:
            return {'is_suspicious': True, 'reason': 'No trading activity'}
            
        avg_volume = sum(volume_windows.values()) / len(volume_windows)
        max_volume = max(volume_windows.values())
        volume_spikes = sum(1 for v in volume_windows.values() if v > avg_volume * 3)
        
        return {
            'is_suspicious': volume_spikes > len(volume_windows) * 0.2,  # >20% spikes
            'metrics': {
                'average_volume': avg_volume,
                'max_volume': max_volume,
                'volume_spikes': volume_spikes,
                'total_windows': len(volume_windows)
            }
        }
        
    async def _analyze_supply_allocation(self, holders: List[Dict], 
                                       total_supply: float,
                                       deployer_address: str) -> Dict:
        """Check for suspicious initial supply allocation"""
        # Get initial allocation transactions
        initial_txs = await self._get_initial_transactions(deployer_address)
        
        # Check for single-entity control
        deployer_balance = sum(float(h['amount']) for h in holders 
                             if h['owner'] == deployer_address)
        deployer_percentage = deployer_balance / total_supply
        
        # Check for suspicious batch transfers
        batch_transfers = self._analyze_batch_patterns(initial_txs)
        
        return {
            'is_suspicious': deployer_percentage > 0.5 or batch_transfers['is_suspicious'],
            'metrics': {
                'deployer_percentage': deployer_percentage,
                'batch_analysis': batch_transfers
            }
        }
        
    def _analyze_batch_patterns(self, transactions: List[Dict]) -> Dict:
        """Analyze patterns in batch transfers for suspicious behavior"""
        if not transactions:
            return {'is_suspicious': True, 'reason': 'No initial transactions'}
            
        # Group transfers by amount
        amount_groups = {}
        for tx in transactions:
            amount = tx['amount']
            if amount not in amount_groups:
                amount_groups[amount] = 0
            amount_groups[amount] += 1
            
        # Check for suspicious patterns
        max_identical = max(amount_groups.values())
        total_transfers = len(transactions)
        
        return {
            'is_suspicious': max_identical > total_transfers * 0.4,  # >40% identical
            'metrics': {
                'max_identical_transfers': max_identical,
                'total_transfers': total_transfers,
                'unique_amounts': len(amount_groups)
            }
        }
        
    async def _get_token_holders(self, token_address: str) -> List[Dict]:
        """Get all token holders using Helius API"""
        payload = {
            "jsonrpc": "2.0",
            "id": "my-id",
            "method": "getTokenLargestAccounts",
            "params": [token_address]
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.post(self.api_url, json=payload) as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get('result', {}).get('value', [])
        return []
        
    async def _get_recent_trades(self, token_address: str) -> List[Dict]:
        """Get recent trades from database"""
        conn = sqlite3.connect(DB_FILE)
        try:
            c = conn.cursor()
            c.execute("""
                SELECT amount, price, timestamp
                FROM transactions
                WHERE token_address = ?
                AND timestamp >= ?
                ORDER BY timestamp DESC
                LIMIT 1000
            """, (token_address, int(time.time()) - 86400))  # Last 24 hours
            
            return [{
                'amount': row[0],
                'price': row[1],
                'timestamp': row[2]
            } for row in c.fetchall()]
        finally:
            conn.close()
            
    async def _get_initial_transactions(self, deployer_address: str) -> List[Dict]:
        """Get initial token transfer transactions"""
        conn = sqlite3.connect(DB_FILE)
        try:
            c = conn.cursor()
            c.execute("""
                SELECT amount
                FROM transactions
                WHERE seller_address = ?
                AND timestamp >= ?
                ORDER BY timestamp ASC
                LIMIT 100
            """, (deployer_address, int(time.time()) - 3600))  # First hour
            
            return [{'amount': row[0]} for row in c.fetchall()]
        finally:
            conn.close()
            
    def _is_burn_address(self, address: str) -> bool:
        """Check if address is a known burn address"""
        burn_addresses = {
            "1111111111111111111111111111111111111111",
            "burn111111111111111111111111111111111111",
            "deadbeef111111111111111111111111111111"
        }
        return address in burn_addresses
            
    def _get_failure_reasons(self, concentration: Dict, 
                           volume: Dict, 
                           supply: Dict) -> List[str]:
        """Get list of reasons why token failed checks"""
        reasons = []
        if concentration['is_suspicious']:
            reasons.append(f"High holder concentration: {concentration['concentration']*100:.1f}%")
        if volume['is_suspicious']:
            reasons.append("Suspicious trading volume patterns")
        if supply['is_suspicious']:
            if supply['metrics']['deployer_percentage'] > 0.5:
                reasons.append("High deployer token control")
            if supply['metrics']['batch_analysis']['is_suspicious']:
                reasons.append("Suspicious batch transfer patterns")
        return reasons
        
    def _get_empty_result(self) -> Dict:
        """Return empty result structure"""
        return {
            'is_safe': False,
            'metrics': {
                'top_10_concentration': {'is_suspicious': True, 'concentration': 0},
                'volume_analysis': {'is_suspicious': True, 'reason': 'No data'},
                'supply_analysis': {'is_suspicious': True, 'reason': 'No data'}
            },
            'failure_reasons': ["Could not analyze token"]
        }

class MarketCapAnalyzer:
    """Analyzes and tracks token market cap in real-time"""
    
    def __init__(self, helius_api_key: str):
        self.helius_api_key = helius_api_key
        self.api_url = f"https://mainnet.helius-rpc.com/?api-key={helius_api_key}"
        self._cache = {}  # Cache market cap calculations
        
    async def get_market_cap(self, token_address: str) -> float:
        """Calculate current market cap for a token"""
        try:
            # Get token metadata including decimals and supply
            metadata = await self._get_token_metadata(token_address)
            if not metadata:
                return 0
                
            # Get current price from Jupiter/Orca
            price = await self._get_token_price(token_address)
            if price == 0:
                return 0
                
            # Calculate market cap
            decimals = metadata.get('decimals', 9)
            supply = float(metadata.get('supply', 0)) / (10 ** decimals)
            market_cap = supply * price
            
            # Cache the result
            self._cache[token_address] = {
                'market_cap': market_cap,
                'timestamp': time.time()
            }
            
            return market_cap
            
        except Exception as e:
            logging.error(f"Error calculating market cap for {token_address}: {e}")
            return 0
            
    async def _get_token_metadata(self, token_address: str) -> dict:
        """Get token metadata from Helius API"""
        try:
            async with aiohttp.ClientSession() as session:
                payload = {
                    "jsonrpc": "2.0",
                    "id": "my-id",
                    "method": "getAsset",
                    "params": {
                        "id": token_address
                    }
                }
                async with session.post(self.api_url, json=payload) as response:
                    if response.status == 200:
                        data = await response.json()
                        return {
                            'decimals': data['result'].get('token_info', {}).get('decimals', 9),
                            'supply': data['result'].get('token_info', {}).get('supply', '0')
                        }
            return None
        except Exception as e:
            logging.error(f"Error fetching token metadata: {e}")
            return None
            
    async def _get_token_price(self, token_address: str) -> float:
        """Get token price from Jupiter API"""
        try:
            # Use Jupiter API to get price
            jupiter_url = f"https://price.jup.ag/v4/price?ids={token_address}"
            async with aiohttp.ClientSession() as session:
                async with session.get(jupiter_url) as response:
                    if response.status == 200:
                        data = await response.json()
                        return float(data.get('data', {}).get(token_address, {}).get('price', 0))
            return 0
        except Exception as e:
            logging.error(f"Error fetching token price: {e}")
            return 0
            
    def is_above_threshold(self, market_cap: float, threshold: float = 30000) -> bool:
        """Check if market cap is above threshold"""
        return market_cap >= threshold

class TokenMonitor:
    def __init__(self, db_file: str):
        self.db_file = db_file
        self.price_alerts: Dict[str, List[PriceAlert]] = {}
        self.market_cap_analyzer = MarketCapAnalyzer(HELIUS_API_KEY)
        self.init_db()
    
    def add_price_alert(self, token_address: str, target_price: float, is_above: bool = True):
        """Add a price alert for a token"""
        if token_address not in self.price_alerts:
            self.price_alerts[token_address] = []
        alert = PriceAlert(token_address, target_price, is_above)
        self.price_alerts[token_address].append(alert)
        logging.info(f"Added price alert for {token_address}: {'above' if is_above else 'below'} {target_price}")
    
    async def check_price_alerts(self, token_address: str, current_price: float):
        """Check if any price alerts have been triggered"""
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
                await self.send_price_alert(token_address, current_price, alert.target_price, alert.is_above)
    
    async def send_price_alert(self, token_address: str, current_price: float, target_price: float, was_above: bool):
        """Send price alert notification"""
        message = (
            f"🚨 **Price Alert!**\n\n"
            f"**Token:** `{token_address}`\n"
            f"**Current Price:** ${current_price:.6f}\n"
            f"**Target {'Above' if was_above else 'Below'}:** ${target_price:.6f}\n\n"
            f"🔗 [View on Solscan](https://solscan.io/token/{token_address})"
        )
        
        webhook = DiscordWebhook(url=DISCORD_WEBHOOK_URL)
        webhook.add_embed({
            "title": "Price Alert Triggered",
            "description": message,
            "color": 0x00ff00 if was_above else 0xff0000
        })
        
        response = webhook.execute()
        if response.status_code != 204:
            logging.error(f"Error sending price alert: {response.text}")

    async def monitor_market_cap(self, token_address: str):
        """Monitor token's market cap and trigger analysis when it crosses 30k"""
        try:
            market_cap = await self.market_cap_analyzer.get_market_cap(token_address)
            if self.market_cap_analyzer.is_above_threshold(market_cap):
                logging.info(f"Token {token_address} reached 30k market cap threshold: ${market_cap:,.2f}")
                
                # Trigger comprehensive analysis
                await self.analyze_token(token_address)
                
                # Send notification
                await self.notify_discord({
                    'token_address': token_address,
                    'market_cap': market_cap,
                    'event': 'market_cap_threshold',
                    'timestamp': time.time()
                })
                
        except Exception as e:
            logging.error(f"Error monitoring market cap for {token_address}: {e}")
            
    async def analyze_token(self, token_address: str):
        """Run comprehensive token analysis"""
        # Get token data
        token_data = await self.get_token_data(token_address)
        if not token_data:
            return
            
        # Run analysis components
        deployer_score = await self.deployer_analyzer.analyze_deployer(token_data['deployer'])
        holder_metrics = await self.holder_analyzer.analyze_token_metrics(token_address, token_data['deployer'])
        volume_analysis = await self.volume_analyzer.analyze_volume(token_address, token_data['recent_trades'])
        
        # Calculate final score
        score = await self.token_scorer.calculate_token_score(token_address, token_data)
        
        # Save results
        await self.save_analysis_results(token_address, {
            'deployer_score': deployer_score,
            'holder_metrics': holder_metrics,
            'volume_analysis': volume_analysis,
            'final_score': score
        })

class WalletTracker:
    """Tracks and manages blacklisted and notable wallets"""
    
    def __init__(self, db_file: str):
        self.db_file = db_file
        self.init_db()
        
    def init_db(self):
        """Initialize wallet tracking tables"""
        conn = sqlite3.connect(self.db_file)
        try:
            c = conn.cursor()
            
            # Blacklisted wallets table
            c.execute("""
                CREATE TABLE IF NOT EXISTS blacklisted_wallets (
                    address TEXT PRIMARY KEY,
                    reason TEXT,
                    evidence TEXT,
                    detection_time INTEGER,
                    confidence_score REAL,
                    detection_method TEXT
                )
            """)
            
            # Notable wallets table (good performers)
            c.execute("""
                CREATE TABLE IF NOT EXISTS notable_wallets (
                    address TEXT PRIMARY KEY,
                    category TEXT,  -- 'trader', 'developer', 'insider'
                    win_rate REAL,
                    avg_roi REAL,
                    total_successful_tokens INTEGER,
                    last_updated INTEGER,
                    notes TEXT
                )
            """)
            
            # Wallet performance history
            c.execute("""
                CREATE TABLE IF NOT EXISTS wallet_performance (
                    address TEXT,
                    token_address TEXT,
                    entry_price REAL,
                    exit_price REAL,
                    profit_loss REAL,
                    hold_duration INTEGER,
                    timestamp INTEGER,
                    PRIMARY KEY (address, token_address)
                )
            """)
            
            conn.commit()
        finally:
            conn.close()
            
    async def add_to_blacklist(self, address: str, reason: str, 
                              evidence: str, confidence: float = 1.0) -> bool:
        """Add wallet to blacklist with reason and evidence"""
        try:
            conn = sqlite3.connect(self.db_file)
            c = conn.cursor()
            
            c.execute("""
                INSERT OR REPLACE INTO blacklisted_wallets
                (address, reason, evidence, detection_time, confidence_score, detection_method)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                address, 
                reason, 
                evidence,
                int(time.time()),
                confidence,
                "automatic_detection"
            ))
            
            conn.commit()
            logging.info(f"Added {address} to blacklist: {reason}")
            return True
            
        except Exception as e:
            logging.error(f"Error adding to blacklist: {e}")
            return False
        finally:
            conn.close()
            
    async def add_notable_wallet(self, address: str, category: str,
                                win_rate: float, avg_roi: float,
                                successful_tokens: int, notes: str = "") -> bool:
        """Add or update notable wallet with performance metrics"""
        try:
            conn = sqlite3.connect(self.db_file)
            c = conn.cursor()
            
            c.execute("""
                INSERT OR REPLACE INTO notable_wallets
                (address, category, win_rate, avg_roi, 
                 total_successful_tokens, last_updated, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                address,
                category,
                win_rate,
                avg_roi,
                successful_tokens,
                int(time.time()),
                notes
            ))
            
            conn.commit()
            logging.info(f"Added/updated notable wallet {address}")
            return True
            
        except Exception as e:
            logging.error(f"Error adding notable wallet: {e}")
            return False
        finally:
            conn.close()
            
    async def update_wallet_performance(self, address: str, 
                                      token_address: str,
                                      entry_price: float,
                                      exit_price: float) -> None:
        """Update wallet's performance record"""
        try:
            profit_loss = (exit_price - entry_price) / entry_price
            
            conn = sqlite3.connect(self.db_file)
            c = conn.cursor()
            
            c.execute("""
                INSERT OR REPLACE INTO wallet_performance
                (address, token_address, entry_price, exit_price,
                 profit_loss, hold_duration, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                address,
                token_address,
                entry_price,
                exit_price,
                profit_loss,
                0,  # We'll calculate duration in a separate update
                int(time.time())
            ))
            
            conn.commit()
            
            # Update notable wallets if performance is exceptional
            await self._check_for_notable_status(address, c)
            
        except Exception as e:
            logging.error(f"Error updating wallet performance: {e}")
        finally:
            conn.close()
            
    async def is_blacklisted(self, address: str) -> Tuple[bool, str]:
        """Check if wallet is blacklisted, returns (is_blacklisted, reason)"""
        conn = sqlite3.connect(self.db_file)
        try:
            c = conn.cursor()
            c.execute("SELECT reason FROM blacklisted_wallets WHERE address = ?", 
                     (address,))
            
            result = c.fetchone()
            return (bool(result), result[0] if result else "")
            
        finally:
            conn.close()
            
    async def get_notable_wallets(self, category: str = None) -> List[Dict]:
        """Get list of notable wallets with their metrics"""
        conn = sqlite3.connect(self.db_file)
        try:
            c = conn.cursor()
            
            if category:
                c.execute("""
                    SELECT address, category, win_rate, avg_roi, 
                           total_successful_tokens, notes
                    FROM notable_wallets
                    WHERE category = ?
                    ORDER BY win_rate DESC, avg_roi DESC
                """, (category,))
            else:
                c.execute("""
                    SELECT address, category, win_rate, avg_roi,
                           total_successful_tokens, notes
                    FROM notable_wallets
                    ORDER BY win_rate DESC, avg_roi DESC
                """)
                
            return [{
                'address': row[0],
                'category': row[1],
                'win_rate': row[2],
                'avg_roi': row[3],
                'successful_tokens': row[4],
                'notes': row[5]
            } for row in c.fetchall()]
            
        finally:
            conn.close()
            
    async def _check_for_notable_status(self, address: str, 
                                      cursor: sqlite3.Cursor) -> None:
        """Check if wallet qualifies for notable status based on performance"""
        # Get wallet's recent performance
        cursor.execute("""
            SELECT 
                COUNT(*) as total_trades,
                AVG(profit_loss) as avg_pl,
                SUM(CASE WHEN profit_loss > 0 THEN 1 ELSE 0 END) as winning_trades
            FROM wallet_performance
            WHERE address = ?
            AND timestamp >= ?
        """, (address, int(time.time()) - (30 * 24 * 60 * 60)))  # Last 30 days
        
        row = cursor.fetchone()
        if not row or row[0] < 10:  # Need at least 10 trades
            return
            
        total_trades, avg_pl, winning_trades = row
        win_rate = winning_trades / total_trades
        
        # If performance is exceptional, add to notable wallets
        if win_rate > 0.7 and avg_pl > 0.5:  # 70% win rate and 50% avg profit
            await self.add_notable_wallet(
                address=address,
                category='trader',
                win_rate=win_rate,
                avg_roi=avg_pl,
                successful_tokens=winning_trades,
                notes="Automatically added based on exceptional performance"
            )

def init_db():
    """Initialize SQLite database with required tables"""
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    
    # Create tables if they don't exist
    c.execute("""
        CREATE TABLE IF NOT EXISTS tokens (
            address TEXT PRIMARY KEY,
            deployer_address TEXT,
            max_market_cap REAL,
            creation_time INTEGER,
            twitter_handle TEXT,
            twitter_name_changes INTEGER DEFAULT 0
        )
    """)
    
    c.execute("""
        CREATE TABLE IF NOT EXISTS token_holders (
            token_address TEXT,
            holder_address TEXT,
            balance REAL,
            last_updated INTEGER,
            PRIMARY KEY (token_address, holder_address)
        )
    """)
    
    c.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            signature TEXT PRIMARY KEY,
            token_address TEXT,
            transaction_type TEXT,
            amount REAL,
            price REAL,
            timestamp INTEGER,
            buyer_address TEXT,
            seller_address TEXT
        )
    """)
    
    c.execute("""
        CREATE TABLE IF NOT EXISTS known_snipers (
            address TEXT PRIMARY KEY,
            detection_time INTEGER,
            confidence_score REAL,
            detection_reason TEXT
        )
    """)
    
    c.execute("""
        CREATE TABLE IF NOT EXISTS known_insiders (
            address TEXT PRIMARY KEY,
            detection_time INTEGER,
            confidence_score REAL,
            detection_reason TEXT
        )
    """)
    
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
    """
    Analyze deployer's previous tokens and their performance.
    Checks:
    - % of tokens that fell below 200k market cap
    - Number of tokens that exceeded 3M market cap
    """
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        
        # Get all tokens by this deployer
        c.execute("""
            SELECT token_address, max_market_cap 
            FROM tokens 
            WHERE deployer_address = ?
        """, (deployer_address,))
        
        tokens = c.fetchall()
        total_tokens = len(tokens)
        
        if total_tokens == 0:
            return {
                'is_safe': True,
                'reason': 'New deployer',
                'stats': {
                    'total_tokens': 0,
                    'tokens_above_3m': 0,
                    'tokens_below_200k': 0,
                    'failure_rate': 0
                }
            }
        
        # Calculate performance metrics
        tokens_above_3m = sum(1 for t in tokens if t[1] >= 3_000_000)
        tokens_below_200k = sum(1 for t in tokens if t[1] < 200_000)
        failure_rate = tokens_below_200k / total_tokens if total_tokens > 0 else 0
        
        return {
            'is_safe': failure_rate < 0.97,  # Less than 97% of tokens below 200k
            'stats': {
                'total_tokens': total_tokens,
                'tokens_above_3m': tokens_above_3m,
                'tokens_below_200k': tokens_below_200k,
                'failure_rate': failure_rate
            }
        }
        
    except Exception as e:
        logging.error(f"Error analyzing deployer history: {e}")
        return {
            'is_safe': False,
            'reason': str(e),
            'stats': {}
        }
    finally:
        conn.close()

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

class WebSocketMonitor:
    def __init__(self, db_file: str):
        self.db_file = db_file
        self._ws_url = "wss://spl_governance.api.mainnet-beta.solana.com/ws"
        self._ws = None
        self._init_db()
        self._start_websocket()

    def _init_db(self):
        """Initialize database tables for WebSocket monitoring"""
        conn = sqlite3.connect(self.db_file)
        try:
            c = conn.cursor()
            
            # Create transactions table
            c.execute('''
                CREATE TABLE IF NOT EXISTS transactions (
                    signature TEXT PRIMARY KEY,
                    block_time INTEGER,
                    fee REAL,
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
        except sqlite3.Error as e:
            logging.error(f"Database error in WebSocketMonitor: {e}")
        finally:
            conn.close()

    def _start_websocket(self):
        """Start the WebSocket connection"""
        try:
            self._ws = websocket.WebSocketApp(self._ws_url, on_open=self._on_open, on_message=self._on_message, on_error=self._on_error, on_close=self._on_close)
            self._ws.run_forever()
        except Exception as e:
            logging.error(f"Error starting WebSocket: {e}")

    def _on_open(self, ws):
        """Handle WebSocket open event"""
        logging.info("WebSocket connection established")
        self._subscribe_to_transactions()

    def _on_message(self, ws, message):
        """Handle incoming WebSocket message"""
        try:
            message = json.loads(message)
            if "params" in message and "result" in message["params"]:
                value = message["params"]["result"]["value"]
                if "account" in value:
                    account_data = value["account"]
                    logging.info(f"Processing account notification for: {value['pubkey']}")
                    
                    # Check if this is a pump.fun token creation
                    if account_data["owner"] == PUMP_FUN_PROGRAM_ID:
                        logging.info("Detected pump.fun token creation")
                        if account_data["data"].startswith(CREATE_INSTRUCTION_DISCRIMINATOR):
                            logging.info("Confirmed pump.fun token creation instruction")
                            # Process the account data
                            self._handle_new_token({
                                "address": value["pubkey"],
                                "data": account_data["data"],
                                "owner": account_data["owner"],
                                "lamports": account_data["lamports"],
                                "signature": ""  # We'll get this from transaction data
                            })
        except Exception as e:
            logging.error(f"Error processing WebSocket message: {e}")

    def _on_error(self, ws, error):
        """Handle WebSocket error event"""
        logging.error(f"WebSocket error: {error}")

    def _on_close(self, ws, close_status_code, close_msg):
        """Handle WebSocket close event"""
        logging.info(f"WebSocket connection closed: {close_status_code} {close_msg}")

    def _subscribe_to_transactions(self):
        """Subscribe to transaction notifications"""
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "subscribe",
            "params": ["accountNotification", {
                "account": "11111111111111111111111111111111",
                "encoding": "jsonParsed",
                "commitment": "finalized"
            }]
        }
        self._ws.send(json.dumps(payload))

    def _handle_new_token(self, token_data):
        """Handle new token creation"""
        try:
            # Get transaction signature
            signature = token_data["signature"]
            tx_data = get_transaction_details(signature)
            if tx_data:
                parsed_data = parse_transaction_data(tx_data)
                if parsed_data:
                    save_transaction_to_db(parsed_data)
                    
                    # Send notification immediately for testing
                    notify_discord(parsed_data)
                    logging.info("Test notification sent.")

                    deployer_address = parsed_data[8]
                    token_address = 'TestToken123'  # In real implementation, extract from tx_data

                    # Calculate and log token score
                    scorer = TokenScorer(DB_FILE)
                    try:
                        score = asyncio.run(scorer.calculate_token_score(token_address, parsed_data))
                        logging.info(f"Token confidence score: {score:.2f}/100")
                    
                    except Exception as e:
                        logging.error(f"Error calculating token score: {e}")

                    # Analyze the deployer history
                    try:
                        result = asyncio.run(analyze_deployer_history(deployer_address))
                        logging.info(f"Deployer history analysis: {result}")
                    except Exception as e:
                        logging.error(f"Error analyzing deployer history: {e}")
        except Exception as e:
            logging.error(f"Error handling new token: {e}")

def main():
    init_db()
    
    sample_signature = "4JWQMMs63xBM3dGKUF29YZnyp6LMEJJGCACo6YBiU2toTqiUDPP79i35Ynct8f6ppCtnRGG7FM7DxomzmYCtuy6F"

    logging.info(f"Fetching transaction: {sample_signature}")
    tx_data = fetch_transaction(sample_signature)

    if tx_data:
        parsed_data = parse_transaction_data(tx_data)
        if parsed_data:
            save_transaction_to_db(parsed_data)
            
            # Send notification immediately for testing
            notify_discord(parsed_data)
            logging.info("Test notification sent.")

            deployer_address = parsed_data[8]
            token_address = 'TestToken123'  # In real implementation, extract from tx_data

            # Calculate and log token score
            scorer = TokenScorer(DB_FILE)
            try:
                score = asyncio.run(scorer.calculate_token_score(token_address, parsed_data))
                logging.info(f"Token confidence score: {score:.2f}/100")
                    
            except Exception as e:
                logging.error(f"Error calculating token score: {e}")

            # Analyze the deployer history
            try:
                result = asyncio.run(analyze_deployer_history(deployer_address))
                logging.info(f"Deployer history analysis: {result}")
            except Exception as e:
                logging.error(f"Error analyzing deployer history: {e}")
        else:
            logging.warning("No valid data to save.")
    else:
        logging.error("Transaction fetch failed.")

    # Run market cap check and deployer analysis
    try:
        asyncio.run(check_market_cap("token_address"))
        result = asyncio.run(analyze_deployer_history(deployer_address))
        logging.info(f"Deployer history analysis: {result}")
    except Exception as e:
        logging.error(f"Error running additional analyses: {e}")

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
        if response.status_code != 204:
            logging.error(f"Error sending notification: {response.text}")
            
    except Exception as e:
        logging.error(f"Error in notify_discord: {e}")

if __name__ == "__main__":
    main()
