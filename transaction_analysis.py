import os
import aiohttp
import asyncio
import logging
from typing import Dict, List
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
HELIUS_API_KEY = os.getenv('HELIUS_API_KEY')
SHYFT_API_KEY = os.getenv('SHYFT_API_KEY')

logging.basicConfig(level=logging.INFO)

class TransactionAnalyzer:
    def __init__(self):
        """Initialize the transaction analyzer."""
        self.helius_base_url = "https://api.helius.xyz/v0"
        self.shyft_base_url = "https://api.shyft.to/sol/v1"
        self.headers = {"x-api-key": SHYFT_API_KEY}

    async def fetch_helius_transactions(self, wallet_address: str, limit: int = 50) -> List[Dict]:
        """
        Fetch recent transactions for a given wallet using the Helius API.
        """
        url = f"{self.helius_base_url}/addresses/{wallet_address}/transactions?api-key={HELIUS_API_KEY}&limit={limit}"
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url) as response:
                    if response.status == 200:
                        transactions = await response.json()
                        logging.info(f"Fetched {len(transactions)} transactions for {wallet_address}")
                        return transactions
                    else:
                        logging.error(f"Failed to fetch transactions: {response.status}")
                        return []
            except Exception as e:
                logging.error(f"Error fetching transactions from Helius: {e}")
                return []

    async def fetch_shyft_transactions(self, wallet_address: str) -> List[Dict]:
        """
        Fetch wallet transactions using the SHYFT API.
        """
        url = f"{self.shyft_base_url}/wallet/transactions"
        params = {"network": "mainnet-beta", "wallet": wallet_address}
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url, headers=self.headers, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        transactions = data.get('result', [])
                        logging.info(f"Fetched {len(transactions)} transactions from SHYFT for {wallet_address}")
                        return transactions
                    else:
                        logging.error(f"Failed to fetch SHYFT transactions: {response.status}")
                        return []
            except Exception as e:
                logging.error(f"Error fetching transactions from SHYFT: {e}")
                return []

    def parse_transactions(self, transactions: List[Dict], token_address: str) -> Dict:
        """
        Analyze transactions and calculate buy/sell ratios for a specific token.
        """
        buy_transactions = []
        sell_transactions = []
        
        for tx in transactions:
            tx_type = tx.get('type', '').upper()
            involved_mint = tx.get('token', {}).get('mint')

            if involved_mint == token_address:
                if tx_type == 'BUY':
                    buy_transactions.append(tx)
                elif tx_type == 'SELL':
                    sell_transactions.append(tx)

        buy_count = len(buy_transactions)
        sell_count = len(sell_transactions)
        total_count = buy_count + sell_count

        buy_volume = sum(float(tx.get('amount', 0)) for tx in buy_transactions)
        sell_volume = sum(float(tx.get('amount', 0)) for tx in sell_transactions)

        # Calculate ratios safely
        buy_sell_ratio = buy_count / sell_count if sell_count > 0 else 1.0
        volume_ratio = buy_volume / sell_volume if sell_volume > 0 else 1.0

        analysis = {
            "buy_count": buy_count,
            "sell_count": sell_count,
            "buy_sell_ratio": round(buy_sell_ratio, 2),
            "buy_volume": buy_volume,
            "sell_volume": sell_volume,
            "volume_ratio": round(volume_ratio, 2),
            "total_transactions": total_count
        }

        logging.info(f"Transaction analysis for {token_address}: {analysis}")
        return analysis

async def test_transaction_analysis():
    """Test the transaction analysis functionality."""
    analyzer = TransactionAnalyzer()
    
    test_wallet = "7Sa6JgAHBKWiEkqW3tJu3umYtSDxcouhRurwQeN1589X"
    test_token = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"

    # Fetch transactions from both APIs
    helius_transactions = await analyzer.fetch_helius_transactions(test_wallet)
    shyft_transactions = await analyzer.fetch_shyft_transactions(test_wallet)

    # Combine transactions from both sources
    all_transactions = helius_transactions + shyft_transactions

    # Analyze transactions
    analysis_result = analyzer.parse_transactions(all_transactions, test_token)
    print("Transaction Analysis Result:", analysis_result)

if __name__ == "__main__":
    asyncio.run(test_transaction_analysis())
