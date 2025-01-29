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
import websockets

# Load environment variables
load_dotenv()
HELIUS_API_KEY = os.getenv("HELIUS_API_KEY")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")
DB_FILE = "solana_transactions.db"

# Constants
PUMP_FUN_PROGRAM_ID = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
CREATE_INSTRUCTION_DISCRIMINATOR = "82a2124e4f31"  # First bytes of create instruction

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

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

    def is_token_safe(self, metrics: TokenMetrics) -> tuple[bool, Dict[str, float], str]:
        """
        Analyze token holder metrics for safety.
        Returns:
            tuple containing:
            - bool: True if token is safe
            - Dict[str, float]: Safety scores for different metrics
            - str: Reason for failure if not safe
        """
        reasons = []
        safety_scores = {}
        
        # Check for sniper concentration
        if metrics.sniper_buys > 2:
            reasons.append("Too many sniper wallets")
            safety_scores["sniper_score"] = 0.0
        else:
            safety_scores["sniper_score"] = 1.0 - (metrics.sniper_buys / 3)
            
        # Check for insider trading
        if metrics.insider_buys > 2:
            reasons.append("Too many insider wallets")
            safety_scores["insider_score"] = 0.0
        else:
            safety_scores["insider_score"] = 1.0 - (metrics.insider_buys / 3)
            
        # Check buy/sell ratio
        buy_sell_ratio = metrics.buy_count / max(metrics.sell_count, 1)
        if buy_sell_ratio > 0.7 and metrics.sell_count < metrics.buy_count * 0.3:
            reasons.append("Suspicious buy/sell ratio")
            safety_scores["trade_ratio_score"] = 0.0
        else:
            safety_scores["trade_ratio_score"] = 1.0 - max(0, (buy_sell_ratio - 0.6) / 0.4)
            
        # Check holder concentration
        if metrics.large_holders > 2:
            reasons.append("Too many large holders")
            safety_scores["holder_concentration_score"] = 0.0
        else:
            safety_scores["holder_concentration_score"] = 1.0 - (metrics.large_holders / 3)
            
        is_safe = len(reasons) == 0
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

class TokenScorer:
    def __init__(self, db_file: str):
        self.db_file = db_file
        self.deployer_analyzer = DeployerAnalyzer(db_file)
        self.holder_analyzer = HolderAnalyzer(db_file)

    async def calculate_token_score(self, token_address: str, tx_data: tuple) -> float:
        """
        Calculate a comprehensive confidence score for a token.
        Returns a score between 0-100, where higher is better.
        """
        try:
            scores = {
                'deployer_score': await self._get_deployer_score(tx_data[8]),  # 30%
                'holder_score': self._get_holder_score(tx_data),               # 25%
                'transaction_score': self._get_transaction_score(tx_data),     # 20%
                'sniper_score': self._get_sniper_score(tx_data),              # 15%
                'market_cap_score': await self._get_market_cap_score(token_address)  # 10%
            }

            # Weight multipliers
            weights = {
                'deployer_score': 0.30,
                'holder_score': 0.25,
                'transaction_score': 0.20,
                'sniper_score': 0.15,
                'market_cap_score': 0.10
            }

            # Calculate weighted score
            final_score = sum(scores[key] * weights[key] for key in scores)

            # Store the score in the database
            await self._save_score(token_address, final_score, scores)

            return float(final_score)  # Ensure we return a float
        except Exception as e:
            logging.error(f"Error in calculate_token_score: {str(e)}")
            raise

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

class PumpFunMonitor:
    """Monitor pump.fun for new token launches and analyze them"""
    
    def __init__(self, db_file: str):
        self.db_file = db_file
        self.ws_url = f"wss://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
        self.init_db()
    
    def init_db(self):
        """Initialize database tables"""
        conn = sqlite3.connect(self.db_file)
        c = conn.cursor()
        
        # Create tables with all required columns
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
    
    async def subscribe_to_program(self):
        """Subscribe to pump.fun program transactions via WebSocket"""
        logging.info("Starting WebSocket connection...")
        while True:
            try:
                async with websockets.connect(self.ws_url) as websocket:
                    subscribe_msg = {
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "programSubscribe",  # Changed from blockSubscribe
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
                            logging.debug(f"Received data: {json.dumps(data, indent=2)}")
                            
                            if "params" in data and "result" in data["params"]:
                                tx = data["params"]["result"]["value"]
                                if "transaction" in tx:
                                    if self.is_pump_fun_creation(tx):
                                        logging.info(f"Found pump.fun creation transaction!")
                                        await self.handle_new_token(tx)
                                        
                        except json.JSONDecodeError as e:
                            logging.error(f"Error decoding WebSocket message: {e}")
                            continue
                            
            except Exception as e:
                logging.error(f"WebSocket connection error: {e}")
                logging.info("Attempting to reconnect in 5 seconds...")
                await asyncio.sleep(5)
                continue
    
    def is_pump_fun_creation(self, tx):
        """Check if transaction is a pump.fun token creation"""
        try:
            # Check if transaction involves pump.fun program
            program_found = False
            for account in tx["transaction"]["message"]["accountKeys"]:
                if account["pubkey"] == PUMP_FUN_PROGRAM_ID:
                    program_found = True
                    break
            
            if not program_found:
                return False
            
            # Look for create instruction in inner instructions
            if "meta" in tx and "innerInstructions" in tx["meta"]:
                for ix in tx["meta"]["innerInstructions"]:
                    if "instructions" in ix:
                        for instruction in ix["instructions"]:
                            # Log the instruction data for debugging
                            logging.debug(f"Instruction data: {json.dumps(instruction, indent=2)}")
                            if "data" in instruction and CREATE_INSTRUCTION_DISCRIMINATOR in instruction["data"]:
                                logging.info("Found create instruction")
                                return True
            
            return False
            
        except Exception as e:
            logging.error(f"Error checking pump.fun creation: {e}")
            return False

async def main():
    """Main entry point"""
    # Configure more detailed logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('pump_fun_monitor.log')
        ]
    )
    
    monitor = PumpFunMonitor(DB_FILE)
    try:
        await monitor.subscribe_to_program()
    except KeyboardInterrupt:
        logging.info("Shutting down monitor...")
    except Exception as e:
        logging.error(f"Fatal error: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())
