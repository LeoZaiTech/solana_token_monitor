import os
import aiohttp
import asyncio
import logging
import json
from typing import Dict, List, Set
from dotenv import load_dotenv, dotenv_values

# Load environment variables from a specific path
env_path = '/Users/zenzai613/CascadeProjects/solana_token_monitor/.env'
env_vars = dotenv_values(env_path)

# Hardcode the API key for testing (replace with env var after verification)
HELIUS_API_KEY = "8952a51d-a041-491a-9835-15408585276e"
SHYFT_API_KEY = env_vars.get('SHYFT_API_KEY')

# Print API key to verify correct loading
print("Loaded Helius API Key (from .env):", env_vars.get('HELIUS_API_KEY'))
print("Using Helius API Key:", HELIUS_API_KEY)


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
        print(f"Requesting URL: {url}")  # Debugging the API call
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url) as response:
                    if response.status == 200:
                        data = await response.json()
                        token_balances = data.get('tokens', [])
                        holders = [
                            {
                                'address': balance.get('tokenAccount'),
                                'amount': float(balance.get('amount', 0)) / (10 ** balance.get('decimals', 0))
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

            high_holders = [
                holder for holder in holders if (holder['amount'] / total_supply) * 100 > 8
            ]

            sniper_wallets = [
                holder for holder in holders if holder['address'] in self.known_sniper_wallets
            ]
            insider_wallets = [
                holder for holder in holders if holder['address'] in self.known_insider_wallets
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
            sorted_holders = sorted(holders, key=lambda x: x['amount'], reverse=True)
            top_10_supply = sum(h['amount'] for h in sorted_holders[:10])
            concentration = (top_10_supply / total_supply) * 100
            return concentration
        except Exception as e:
            logging.error(f"Error calculating concentration score: {e}")
            return 100.0

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


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(test_holder_analysis())
