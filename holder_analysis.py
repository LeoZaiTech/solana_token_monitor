import os
import aiohttp
import asyncio
import logging
import json
from typing import Dict, List, Set
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
HELIUS_API_KEY = os.getenv('HELIUS_API_KEY')
SHYFT_API_KEY = os.getenv('SHYFT_API_KEY')

class HolderAnalyzer:
    def __init__(self):
        """Initialize holder analyzer with wallet tracking."""
        self.known_sniper_wallets: Set[str] = set()
        self.known_insider_wallets: Set[str] = set()
        self.load_wallet_data()

    def load_wallet_data(self):
        """Load known wallet data from JSON file."""
        try:
            with open('wallet_data.json', 'r') as f:
                data = json.load(f)
                self.known_sniper_wallets = set(data.get('sniper_wallets', []))
                self.known_insider_wallets = set(data.get('insider_wallets', []))
                logging.info(f"Loaded {len(self.known_sniper_wallets)} sniper wallets and {len(self.known_insider_wallets)} insider wallets")
        except FileNotFoundError:
            logging.warning("wallet_data.json not found, initializing empty sets")
            self.save_wallet_data()  # Create initial file

    def save_wallet_data(self):
        """Save known wallet data to JSON file."""
        data = {
            'sniper_wallets': list(self.known_sniper_wallets),
            'insider_wallets': list(self.known_insider_wallets)
        }
        with open('wallet_data.json', 'w') as f:
            json.dump(data, f, indent=2)
        logging.info("Wallet data saved successfully")

    async def fetch_token_holders(self, token_address: str) -> List[Dict]:
        """Fetch token holders using Helius API."""
        url = f"https://api.helius.xyz/v0/addresses/{token_address}/balances?api-key={HELIUS_API_KEY}"
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url) as response:
                    if response.status == 200:
                        data = await response.json()
                        # Extract token balances
                        token_balances = data.get('tokens', [])
                        holders = [
                            {
                                'address': balance.get('owner'),
                                'amount': float(balance.get('amount', 0))
                            }
                            for balance in token_balances
                            if balance.get('mint') == token_address
                        ]
                        logging.info(f"Fetched {len(holders)} holders for token {token_address}")
                        return holders
                    else:
                        logging.error(f"Failed to fetch holders: Status {response.status}")
                        logging.error(f"Response: {await response.text()}")
                        return []
            except Exception as e:
                logging.error(f"Error fetching token holders: {e}")
                return []

    def analyze_holder_distribution(self, holders: List[Dict]) -> Dict:
        """Analyze the token holder distribution and identify potential issues."""
        try:
            total_supply = sum(holder['amount'] for holder in holders)
            if total_supply == 0:
                logging.error("Total supply is 0, cannot analyze distribution")
                return self._empty_analysis_result()

            # Identify high holders (>8% of supply)
            high_holders = [
                holder for holder in holders 
                if (holder['amount'] / total_supply) * 100 > 8
            ]

            # Check for known problematic wallets
            sniper_wallets = [
                holder for holder in holders 
                if holder['address'] in self.known_sniper_wallets
            ]
            insider_wallets = [
                holder for holder in holders 
                if holder['address'] in self.known_insider_wallets
            ]

            analysis_result = {
                "total_holders": len(holders),
                "high_holders": len(high_holders),
                "high_holder_addresses": [h['address'] for h in high_holders],
                "sniper_wallets": len(sniper_wallets),
                "sniper_addresses": [w['address'] for w in sniper_wallets],
                "insider_wallets": len(insider_wallets),
                "insider_addresses": [w['address'] for w in insider_wallets],
                "concentration_score": self._calculate_concentration_score(holders, total_supply)
            }

            logging.info(f"Holder analysis completed: {analysis_result}")
            return analysis_result
        except Exception as e:
            logging.error(f"Error in analyze_holder_distribution: {e}")
            return self._empty_analysis_result()

    def _calculate_concentration_score(self, holders: List[Dict], total_supply: int) -> float:
        """Calculate a score representing how concentrated the token holdings are."""
        try:
            # Sort holders by amount descending
            sorted_holders = sorted(holders, key=lambda x: x['amount'], reverse=True)
            
            # Calculate percentage of supply held by top 10 holders
            top_10_supply = sum(h['amount'] for h in sorted_holders[:10])
            concentration = (top_10_supply / total_supply) * 100

            return concentration
        except Exception as e:
            logging.error(f"Error calculating concentration score: {e}")
            return 100.0  # Return max concentration on error

    async def fetch_wallet_transactions(self, wallet_address: str, token_address: str) -> List[Dict]:
        """Fetch wallet transaction history using SHYFT API."""
        url = f"https://api.shyft.to/sol/v1/wallet/transactions"
        params = {
            "network": "mainnet-beta",
            "wallet": wallet_address,
            "token_address": token_address
        }
        headers = {"x-api-key": SHYFT_API_KEY}

        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url, headers=headers, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        transactions = data.get('result', [])
                        logging.info(f"Fetched {len(transactions)} transactions for wallet {wallet_address}")
                        return transactions
                    else:
                        logging.error(f"Failed to fetch transactions: {response.status}")
                        return []
            except Exception as e:
                logging.error(f"Error fetching wallet transactions: {e}")
                return []

    def analyze_developer_sales(self, transactions: List[Dict], token_address: str) -> Dict:
        """Analyze developer token sales patterns."""
        try:
            # Filter sales transactions
            sales = [
                tx for tx in transactions 
                if tx['type'] == 'SELL' and tx['destination'] != token_address
            ]
            
            total_sale_amount = sum(tx['amount'] for tx in sales)
            
            analysis = {
                "has_sales": len(sales) > 0,
                "total_sales": len(sales),
                "total_sale_amount": total_sale_amount,
                "recent_sales": [
                    {
                        "amount": tx['amount'],
                        "timestamp": tx['timestamp'],
                        "destination": tx['destination']
                    }
                    for tx in sales[-5:]  # Last 5 sales
                ]
            }
            
            logging.info(f"Developer sales analysis: {analysis}")
            return analysis
        except Exception as e:
            logging.error(f"Error analyzing developer sales: {e}")
            return {"has_sales": False, "total_sales": 0, "total_sale_amount": 0, "recent_sales": []}

    def calculate_buy_sell_ratio(self, transactions: List[Dict]) -> Dict:
        """Calculate detailed buy/sell ratios from transaction data."""
        try:
            buy_txs = [tx for tx in transactions if tx['type'] == 'BUY']
            sell_txs = [tx for tx in transactions if tx['type'] == 'SELL']
            
            buy_count = len(buy_txs)
            sell_count = len(sell_txs)
            
            # Calculate ratio (avoid division by zero)
            ratio = buy_count / sell_count if sell_count > 0 else 1.0
            
            # Calculate volumes
            buy_volume = sum(tx['amount'] for tx in buy_txs)
            sell_volume = sum(tx['amount'] for tx in sell_txs)
            
            analysis = {
                "buy_count": buy_count,
                "sell_count": sell_count,
                "ratio": ratio,
                "buy_volume": buy_volume,
                "sell_volume": sell_volume,
                "volume_ratio": buy_volume / sell_volume if sell_volume > 0 else 1.0
            }
            
            logging.info(f"Buy/Sell ratio analysis: {analysis}")
            return analysis
        except Exception as e:
            logging.error(f"Error calculating buy/sell ratio: {e}")
            return {
                "buy_count": 0,
                "sell_count": 0,
                "ratio": 1.0,
                "buy_volume": 0,
                "sell_volume": 0,
                "volume_ratio": 1.0
            }

    def _empty_analysis_result(self) -> Dict:
        """Return empty analysis result structure."""
        return {
            "total_holders": 0,
            "high_holders": 0,
            "high_holder_addresses": [],
            "sniper_wallets": 0,
            "sniper_addresses": [],
            "insider_wallets": 0,
            "insider_addresses": [],
            "concentration_score": 100.0
        }

async def test_holder_analysis():
    """Test the holder analysis functionality."""
    analyzer = HolderAnalyzer()
    
    # Test with a sample token (USDC on Solana)
    test_token = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
    
    print("\nFetching token holders...")
    # First get token metadata
    url = f"https://api.helius.xyz/v0/token-metadata?api-key={HELIUS_API_KEY}"
    async with aiohttp.ClientSession() as session:
        async with session.post(url, json={"mintAccounts": [test_token]}) as response:
            if response.status == 200:
                metadata = await response.json()
                print(f"\nToken Metadata: {json.dumps(metadata, indent=2)}")

    holders = await analyzer.fetch_token_holders(test_token)
    
    if holders:
        print("\nAnalyzing holder distribution...")
        analysis = analyzer.analyze_holder_distribution(holders)
        print(f"\nAnalysis Results:")
        print(json.dumps(analysis, indent=2))
        
        # Test transaction analysis if we have holders
        if analysis['total_holders'] > 0:
            print("\nFetching recent transactions...")
            sample_holder = holders[0]['address']
            transactions = await analyzer.fetch_wallet_transactions(
                sample_holder,
                test_token
            )
            
            if transactions:
                print("\nAnalyzing buy/sell patterns...")
                ratio_analysis = analyzer.calculate_buy_sell_ratio(transactions)
                print(json.dumps(ratio_analysis, indent=2))

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(test_holder_analysis())
