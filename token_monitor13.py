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
    def __init__(self):
        self.bearer_token = os.getenv("TWITTER_BEARER_TOKEN")
        self.api_base_url = "https://api.twitter.com/2"
        if not self.bearer_token:
            logging.warning("TWITTER_BEARER_TOKEN not set. Twitter analysis will be disabled.")
        self.session = None

    async def analyze_token(self, token_address: str) -> Optional[Dict]:
        """Analyze Twitter presence for a token"""
        if not self.bearer_token:
            return None
        return {'sentiment_score': 0.5}  # Placeholder for now

    def calculate_risk_score(self, metrics: Dict) -> float:
        """Calculate risk score from Twitter metrics"""
        return 0.5  # Neutral score for now

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
            twitter_metrics = await self.twitter_analyzer.analyze_token(token_address)
            if not twitter_metrics:
                return 50.0  # Neutral score if no Twitter data

            risk_score = self.twitter_analyzer.calculate_risk_score(twitter_metrics)
            
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

class PriceAlert:
    def __init__(self, token_address: str, target_price: float, is_above: bool = True):
        self.token_address = token_address
        self.target_price = target_price
        self.is_above = is_above  # True for price above target, False for below
        self.triggered = False

class TokenMonitor:
    def __init__(self, db_file: str):
        self.db_file = db_file
        self.price_alerts: Dict[str, List[PriceAlert]] = {}
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

def init_db():
    """Initialize database schema for token monitoring"""
    conn = sqlite3.connect(DB_FILE)
    try:
        c = conn.cursor()
        
        # Create tokens table
        c.execute('''
            CREATE TABLE IF NOT EXISTS tokens (
                address TEXT PRIMARY KEY,
                name TEXT,
                symbol TEXT,
                decimals INTEGER,
                current_price REAL,
                current_market_cap REAL,
                peak_market_cap REAL,
                last_updated INTEGER
            )
        ''')
        
        # Create token_market_caps table for historical data
        c.execute('''
            CREATE TABLE IF NOT EXISTS token_market_caps (
                token_address TEXT,
                market_cap REAL,
                timestamp INTEGER,
                PRIMARY KEY (token_address, timestamp)
            )
        ''')
        
        # Create deployer_stats table
        c.execute('''
            CREATE TABLE IF NOT EXISTS deployer_stats (
                address TEXT PRIMARY KEY,
                total_tokens INTEGER,
                tokens_above_3m INTEGER,
                tokens_above_200k INTEGER,
                last_token_time INTEGER,
                is_blacklisted BOOLEAN,
                last_updated INTEGER,
                failure_rate REAL
            )
        ''')
        
        # Create token_scores table
        c.execute('''
            CREATE TABLE IF NOT EXISTS token_scores (
                token_address TEXT PRIMARY KEY,
                final_score REAL,
                component_scores TEXT,
                last_updated INTEGER
            )
        ''')
        
        conn.commit()
    finally:
        conn.close()

def ensure_db_columns():
    """Ensure required columns exist in the deployers and tokens tables"""
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    try:
        # Check existing columns in tokens table
        c.execute("PRAGMA table_info(tokens)")
        token_columns = [row[1] for row in c.fetchall()]

        # Add Twitter-related columns if they don't exist
        if "twitter_handle" not in token_columns:
            c.execute("ALTER TABLE tokens ADD COLUMN twitter_handle TEXT")
            logging.info("Added column 'twitter_handle' to tokens table")

        if "twitter_name_changes" not in token_columns:
            c.execute("ALTER TABLE tokens ADD COLUMN twitter_name_changes INTEGER DEFAULT 0")
            logging.info("Added column 'twitter_name_changes' to tokens table")

        if "twitter_followers" not in token_columns:
            c.execute("ALTER TABLE tokens ADD COLUMN twitter_followers INTEGER DEFAULT 0")
            logging.info("Added column 'twitter_followers' to tokens table")

        if "twitter_creation_date" not in token_columns:
            c.execute("ALTER TABLE tokens ADD COLUMN twitter_creation_date INTEGER")
            logging.info("Added column 'twitter_creation_date' to tokens table")

        if "twitter_sentiment_score" not in token_columns:
            c.execute("ALTER TABLE tokens ADD COLUMN twitter_sentiment_score REAL DEFAULT 0")
            logging.info("Added column 'twitter_sentiment_score' to tokens table")

        if "twitter_engagement_score" not in token_columns:
            c.execute("ALTER TABLE tokens ADD COLUMN twitter_engagement_score REAL DEFAULT 0")
            logging.info("Added column 'twitter_engagement_score' to tokens table")

        if "twitter_risk_score" not in token_columns:
            c.execute("ALTER TABLE tokens ADD COLUMN twitter_risk_score REAL DEFAULT 0")
            logging.info("Added column 'twitter_risk_score' to tokens table")

        if "twitter_last_updated" not in token_columns:
            c.execute("ALTER TABLE tokens ADD COLUMN twitter_last_updated INTEGER")
            logging.info("Added column 'twitter_last_updated' to tokens table")

        # Create twitter_mentions table if it doesn't exist
        c.execute('''
            CREATE TABLE IF NOT EXISTS twitter_mentions (
                token_address TEXT,
                tweet_id TEXT,
                author_id TEXT,
                created_at INTEGER,
                sentiment_score REAL,
                is_notable_account INTEGER,
                PRIMARY KEY (token_address, tweet_id)
            )
        ''')
        logging.info("Ensured twitter_mentions table exists")

        conn.commit()
    except sqlite3.Error as e:
        logging.error(f"Database error: {e}")
    finally:
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
                        asyncio.run(analyze_deployer_history(deployer_address))
                    except Exception as e:
                        logging.error(f"Error analyzing deployer history: {e}")
        except Exception as e:
            logging.error(f"Error handling new token: {e}")

def main():
    init_db()
    ensure_db_columns()
    
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
