import os
import json
import time
import logging
import requests
from typing import Dict, List, Optional
from dotenv import load_dotenv

load_dotenv()

class APIUtils:
    def __init__(self):
        self.helius_api_key = os.getenv('HELIUS_API_KEY')
        self.bitquery_api_key = os.getenv('BITQUERY_API_KEY')
        self.solscan_api_key = os.getenv('SOLSCAN_API_KEY')
        self.shyft_api_key = os.getenv('SHYFT_API_KEY')
        
        # API endpoints
        self.helius_base_url = "https://api.helius.xyz/v0"
        self.bitquery_base_url = "https://graphql.bitquery.io"
        self.solscan_base_url = "https://public-api.solscan.io"
        self.shyft_base_url = "https://api.shyft.to/sol/v1"

    def get_token_metadata(self, token_address: str) -> Optional[Dict]:
        """Fetch token metadata from Helius."""
        try:
            url = f"{self.helius_base_url}/token-metadata?api-key={self.helius_api_key}"
            response = requests.post(url, json={"mintAccounts": [token_address]})
            if response.status_code == 200:
                return response.json()[0]
            logging.error(f"Failed to fetch token metadata: {response.status_code}")
            return None
        except Exception as e:
            logging.error(f"Error fetching token metadata: {e}")
            return None

    def get_wallet_holdings(self, wallet_address: str) -> Optional[Dict]:
        """Fetch wallet holdings from SHYFT API."""
        try:
            url = f"{self.shyft_base_url}/wallet/all_tokens"
            headers = {"x-api-key": self.shyft_api_key}
            params = {"network": "mainnet-beta", "wallet": wallet_address}
            response = requests.get(url, headers=headers, params=params)
            if response.status_code == 200:
                return response.json()
            logging.error(f"Failed to fetch wallet holdings: {response.status_code}")
            return None
        except Exception as e:
            logging.error(f"Error fetching wallet holdings: {e}")
            return None

    def get_token_holders(self, token_address: str) -> Optional[List[Dict]]:
        """Fetch token holders from Solscan."""
        try:
            url = f"{self.solscan_base_url}/token/holders"
            headers = {"token": self.solscan_api_key}
            params = {"tokenAddress": token_address}
            response = requests.get(url, headers=headers, params=params)
            if response.status_code == 200:
                return response.json()
            logging.error(f"Failed to fetch token holders: {response.status_code}")
            return None
        except Exception as e:
            logging.error(f"Error fetching token holders: {e}")
            return None

    def get_wallet_transactions(self, wallet_address: str, limit: int = 100) -> Optional[List[Dict]]:
        """Fetch wallet transactions from Helius."""
        try:
            url = f"{self.helius_base_url}/addresses/{wallet_address}/transactions?api-key={self.helius_api_key}&limit={limit}"
            response = requests.get(url)
            if response.status_code == 200:
                return response.json()
            logging.error(f"Failed to fetch wallet transactions: {response.status_code}")
            return None
        except Exception as e:
            logging.error(f"Error fetching wallet transactions: {e}")
            return None

    def get_token_price_history(self, token_address: str) -> Optional[List[Dict]]:
        """Fetch token price history using Bitquery."""
        query = """
        query ($token: String!) {
          solana {
            dexTrades(
              options: {limit: 100, desc: "block.timestamp.time"}
              baseCurrency: {is: $token}
            ) {
              timeInterval {
                minute(count: 15)
              }
              baseAmount
              quoteCurrency {
                symbol
              }
              quoteAmount
              block {
                timestamp {
                  time(format: "%Y-%m-%d %H:%M:%S")
                }
              }
            }
          }
        }
        """
        try:
            headers = {"X-API-KEY": self.bitquery_api_key}
            response = requests.post(
                self.bitquery_base_url,
                json={"query": query, "variables": {"token": token_address}},
                headers=headers
            )
            if response.status_code == 200:
                return response.json()["data"]["solana"]["dexTrades"]
            logging.error(f"Failed to fetch token price history: {response.status_code}")
            return None
        except Exception as e:
            logging.error(f"Error fetching token price history: {e}")
            return None
