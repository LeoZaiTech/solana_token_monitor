import os
from dotenv import load_dotenv
import json
import aiohttp
import logging
import asyncio
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Callable, Set
from pydantic import BaseModel, Field
from tenacity import retry, stop_after_attempt, wait_exponential
from aiohttp import ClientResponseError, ClientConnectorError
import sqlite3
from pathlib import Path
import aiohttp.client_exceptions

load_dotenv()

class TokenTrade(BaseModel):
    """Model for token trade data"""
    block_time: datetime
    signature: str
    token_symbol: str
    token_address: str
    amount: float
    price_usd: float
    side: str
    trader: str

class TokenStats(BaseModel):
    """Model for token statistics"""
    trade_amount: float
    trades_count: int
    volume_usd: float
    unique_traders: int
    first_trade_time: datetime
    last_trade_time: datetime

class TokenCreation(BaseModel):
    """Model for new token creation data"""
    signature: str
    mint_address: str
    owner_address: str
    program_id: str
    method: str
    timestamp: datetime

class BitqueryError(Exception):
    """Custom exception for Bitquery API errors"""
    def __init__(self, status: int, message: str):
        self.status = status
        self.message = message
        super().__init__(f"Bitquery API error (status {status}): {message}")

class TokenFilter:
    """Filter configuration for token monitoring"""
    def __init__(self):
        self.blacklisted_owners: Set[str] = set()
        self.blacklisted_mints: Set[str] = set()
        self.required_program_ids = {"TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"}
        
    def is_valid_token(self, token: TokenCreation) -> bool:
        """
        Validate a token creation event against filtering rules.
        
        Returns:
            bool: True if token passes all filters, False otherwise
        """
        if token.owner_address in self.blacklisted_owners:
            logging.warning(f"Token {token.mint_address} ignored: blacklisted owner {token.owner_address}")
            return False
            
        if token.mint_address in self.blacklisted_mints:
            logging.warning(f"Token {token.mint_address} ignored: blacklisted mint")
            return False
            
        if token.program_id not in self.required_program_ids:
            logging.warning(f"Token {token.mint_address} ignored: invalid program ID {token.program_id}")
            return False
            
        return True

class TokenDatabase:
    """SQLite database for storing token creation events"""
    def __init__(self, db_path: str = "tokens.db"):
        self.db_path = db_path
        self._init_db()
        
    def _init_db(self):
        """Initialize the database schema"""
        conn = sqlite3.connect(self.db_path)
        try:
            c = conn.cursor()
            c.execute('''
                CREATE TABLE IF NOT EXISTS token_creations (
                    signature TEXT PRIMARY KEY,
                    mint_address TEXT NOT NULL,
                    owner_address TEXT NOT NULL,
                    program_id TEXT NOT NULL,
                    method TEXT NOT NULL,
                    timestamp DATETIME NOT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            conn.commit()
        finally:
            conn.close()
            
    def save_token(self, token: TokenCreation):
        """Save a token creation event to the database"""
        conn = sqlite3.connect(self.db_path)
        try:
            c = conn.cursor()
            c.execute('''
                INSERT OR IGNORE INTO token_creations 
                (signature, mint_address, owner_address, program_id, method, timestamp)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                token.signature,
                token.mint_address,
                token.owner_address,
                token.program_id,
                token.method,
                token.timestamp.isoformat()
            ))
            conn.commit()
        finally:
            conn.close()

class PumpAPI:
    """Interface with Pump Fun API using Bitquery's GraphQL API."""
    
    def __init__(self, api_key: Optional[str] = None, max_retries: int = 3):
        self.base_url = "https://graphql.bitquery.io"
        self.api_key = api_key if api_key else os.getenv("BITQUERY_API_KEY", "")
        self.max_retries = max_retries
        self.semaphore = asyncio.Semaphore(5)  # Limit concurrent requests
        self._ws = None
        self.token_filter = TokenFilter()
        self.token_db = TokenDatabase()
        
        if not self.api_key:
            raise ValueError("Bitquery API key not found")

    async def _make_request(self, query: str, variables: Dict[str, Any]) -> Dict:
        """Make a GraphQL request with retry logic and rate limiting"""
        async with self.semaphore:  # Rate limiting
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        self.base_url,
                        headers={
                            "Content-Type": "application/json",
                            "X-API-KEY": self.api_key
                        },
                        json={"query": query, "variables": variables}
                    ) as response:
                        if response.status == 200:
                            data = await response.json()
                            if "errors" in data:
                                raise BitqueryError(400, data["errors"][0]["message"])
                            return data
                        elif response.status == 429:
                            retry_after = int(response.headers.get("Retry-After", "60"))
                            logging.warning(f"Rate limited. Waiting {retry_after} seconds")
                            await asyncio.sleep(retry_after)
                            raise BitqueryError(429, "Rate limit exceeded")
                        else:
                            raise BitqueryError(
                                response.status,
                                await response.text()
                            )
            except ClientConnectorError as e:
                logging.error(f"Connection error: {e}")
                raise BitqueryError(503, "Service unavailable")
            except asyncio.TimeoutError:
                logging.error("Request timed out")
                raise BitqueryError(504, "Request timeout")

    async def _test_api_key(self):
        """Test the API key with a simple query"""
        query = """
        query {
          solana {
            instructions(
              options: {
                limit: 1,
                desc: "block.timestamp.time"
              }
            ) {
              block {
                timestamp {
                  time
                }
              }
              transaction {
                signature
              }
              program {
                id
                name
              }
            }
          }
        }
        """
        
        try:
            data = await self._make_request(query, {})
            if data and "data" in data:
                logging.info("API key is valid")
                return True
        except BitqueryError as e:
            logging.error(f"API key validation failed: {e}")
        except Exception as e:
            logging.error(f"Unexpected error during API key validation: {e}")
        return False

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry_error_callback=lambda _: None
    )
    async def get_token_trades(
        self,
        token_address: str,
        limit: int = 50,
        cursor: Optional[str] = None
    ) -> Optional[List[TokenTrade]]:
        """Get recent trades for a specific token with pagination."""
        query = """
        query ($token: String!, $limit: Int!) {
          Solana {
            DEXTrades(
              limit: {count: $limit}
              orderBy: {descending: Block_Time}
              where: {
                Trade: {
                  Currency: {MintAddress: {is: $token}},
                  Dex: {ProtocolName: {is: "pump"}}
                }
              }
            ) {
              Block {
                Time
              }
              Trade {
                Currency {
                  Symbol
                  MintAddress
                }
                Amount
                Price
                Side {
                  Type
                  Account {
                    Address
                  }
                }
              }
              Transaction {
                Signature
              }
            }
          }
        }
        """

        variables = {
            "token": token_address,
            "limit": limit
        }

        try:
            data = await self._make_request(query, variables)
            trades_data = data.get("data", {}).get("Solana", {}).get("DEXTrades", [])
            
            trades = []
            for trade in trades_data:
                trades.append(TokenTrade(
                    block_time=datetime.fromisoformat(trade["Block"]["Time"]),
                    signature=trade["Transaction"]["Signature"],
                    token_symbol=trade["Trade"]["Currency"]["Symbol"],
                    token_address=trade["Trade"]["Currency"]["MintAddress"],
                    amount=float(trade["Trade"]["Amount"]),
                    price_usd=float(trade["Trade"]["Price"]),
                    side=trade["Trade"]["Side"]["Type"],
                    trader=trade["Trade"]["Side"]["Account"]["Address"]
                ))
            return trades
        except BitqueryError as e:
            logging.error(f"Failed to fetch trades: {e}")
            return None
        except Exception as e:
            logging.error(f"Unexpected error fetching trades: {e}")
            return None

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry_error_callback=lambda _: None
    )
    async def get_token_stats(self, token_address: str) -> Optional[TokenStats]:
        """Get token statistics including trading volume."""
        one_hour_ago = datetime.now(timezone.utc) - timedelta(hours=1)
        
        query = """
        query ($token: String!, $since: ISO8601DateTime!) {
          Solana {
            DEXTrades(
              where: {
                Trade: {
                  Currency: {MintAddress: {is: $token}},
                  Dex: {ProtocolName: {is: "pump"}}
                },
                Block: {Time: {since: $since}}
              }
            ) {
              tradeAmount: sum(of: Trade_Amount)
              volumeUSD: sum(of: Trade_AmountUSD)
              tradeCount: count
              uniqueTraders: count(distinct: Trade_Side_Account_Address)
              firstTrade: minimum(of: Block_Time)
              lastTrade: maximum(of: Block_Time)
            }
          }
        }
        """

        variables = {
            "token": token_address,
            "since": one_hour_ago.isoformat()
        }

        try:
            data = await self._make_request(query, variables)
            stats_data = data.get("data", {}).get("Solana", {}).get("DEXTrades", {})
            
            return TokenStats(
                trade_amount=float(stats_data["tradeAmount"]),
                trades_count=int(stats_data["tradeCount"]),
                volume_usd=float(stats_data["volumeUSD"]),
                unique_traders=int(stats_data["uniqueTraders"]),
                first_trade_time=datetime.fromisoformat(stats_data["firstTrade"]),
                last_trade_time=datetime.fromisoformat(stats_data["lastTrade"])
            )
        except BitqueryError as e:
            logging.error(f"Failed to fetch token stats: {e}")
            return None
        except Exception as e:
            logging.error(f"Unexpected error fetching token stats: {e}")
            return None

    async def _handle_ws_message(self, msg: aiohttp.WSMessage, callback: Callable[[TokenCreation], None]):
        """Handle incoming WebSocket messages"""
        if msg.type == aiohttp.WSMsgType.TEXT:
            try:
                data = json.loads(msg.data)
                
                if data["type"] == "data":
                    instruction_data = data["payload"]["data"]["solana"]["instructions"][0]
                    
                    token_data = TokenCreation(
                        signature=instruction_data["transaction"]["signature"],
                        mint_address=instruction_data["accounts"][0]["address"],
                        owner_address=instruction_data["accounts"][0]["address"],
                        program_id=instruction_data["program"]["id"],
                        method=instruction_data["program"]["name"],
                        timestamp=datetime.now(timezone.utc)
                    )
                    
                    # Apply filtering
                    if self.token_filter.is_valid_token(token_data):
                        # Save to database
                        self.token_db.save_token(token_data)
                        # Notify callback
                        callback(token_data)
                    
            except json.JSONDecodeError as e:
                logging.error(f"Failed to parse WebSocket message: {e}")
            except KeyError as e:
                logging.error(f"Unexpected message format: {e}")
            except Exception as e:
                logging.error(f"Error processing message: {e}")
                
        elif msg.type == aiohttp.WSMsgType.ERROR:
            logging.error(f"WebSocket error: {msg.data}")
            raise aiohttp.client_exceptions.WSServerHandshakeError(
                status=None, message=f"WebSocket error: {msg.data}"
            )
        elif msg.type == aiohttp.WSMsgType.CLOSED:
            logging.warning("WebSocket connection closed")
            raise aiohttp.client_exceptions.WSServerHandshakeError(
                status=None, message="WebSocket connection closed"
            )

    async def subscribe_to_new_tokens(self, callback: Callable[[TokenCreation], None]):
        """
        Subscribe to new token creation events with automatic reconnection.
        
        Args:
            callback: Function to call when a new valid token is created.
        """
        # First verify API key
        if not await self._test_api_key():
            raise ValueError("Invalid or unauthorized API key")
            
        query = """
        subscription {
          solana {
            instructions(
              options: {
                limit: 10,
                desc: "block.timestamp.time"
              }
            ) {
              block {
                timestamp {
                  time
                }
              }
              transaction {
                signature
              }
              program {
                id
                name
              }
              inner {
                program {
                  id
                  name
                }
              }
            }
          }
        }
        """

        ws_url = "wss://streaming.bitquery.io/graphql"
        retry_count = 0
        max_retries = 5
        base_delay = 1  # Start with 1 second delay
        
        while True:
            try:
                async with aiohttp.ClientSession() as session:
                    # Use increased timeout and proper headers
                    async with session.ws_connect(
                        ws_url,
                        headers={
                            "X-API-KEY": self.api_key,
                            "Content-Type": "application/json",
                            "Accept": "application/json",
                            "Sec-WebSocket-Protocol": "graphql-ws"
                        },
                        timeout=60,  # Increased timeout
                        heartbeat=30  # Keep connection alive
                    ) as ws:
                        self._ws = ws
                        retry_count = 0  # Reset retry count on successful connection
                        
                        # Send connection init message
                        await ws.send_json({
                            "type": "connection_init",
                            "payload": {
                                "X-API-KEY": self.api_key
                            }
                        })
                        
                        # Wait for connection ack
                        ack = await ws.receive_json()
                        if ack.get("type") != "connection_ack":
                            raise Exception(f"Failed to initialize WebSocket connection: {ack}")
                        
                        # Start subscription
                        await ws.send_json({
                            "id": "1",
                            "type": "start",
                            "payload": {
                                "query": query
                            }
                        })

                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                data = json.loads(msg.data)
                                
                                # Handle different message types
                                msg_type = data.get("type")
                                if msg_type == "data":
                                    await self._handle_ws_message(msg, callback)
                                elif msg_type == "complete":
                                    logging.info("Subscription completed")
                                    break
                                elif msg_type == "error":
                                    logging.error(f"Subscription error: {data.get('payload')}")
                                    break
                                elif msg_type == "ka":  # Keep alive
                                    continue
                            elif msg.type == aiohttp.WSMsgType.ERROR:
                                logging.error(f"WebSocket error: {msg.data}")
                                break
                            elif msg.type == aiohttp.WSMsgType.CLOSED:
                                logging.warning("WebSocket connection closed")
                                break
                            
            except (
                aiohttp.client_exceptions.WSServerHandshakeError,
                aiohttp.client_exceptions.ClientConnectorError,
                asyncio.TimeoutError
            ) as e:
                retry_count += 1
                if retry_count > max_retries:
                    logging.error("Max retries exceeded, stopping subscription")
                    break
                    
                delay = min(base_delay * (2 ** (retry_count - 1)), 60)  # Exponential backoff, max 60 seconds
                logging.warning(f"WebSocket connection failed, retrying in {delay} seconds... Error: {e}")
                await asyncio.sleep(delay)
                
            except Exception as e:
                logging.error(f"Unexpected error in WebSocket connection: {e}")
                retry_count += 1
                if retry_count > max_retries:
                    break
                await asyncio.sleep(5)
            
            finally:
                if self._ws and not self._ws.closed:
                    await self._ws.close()
                self._ws = None

    async def stop_subscription(self):
        """Stop the WebSocket subscription if active."""
        if self._ws and not self._ws.closed:
            await self._ws.close()
            self._ws = None

class PumpFunAPI:
    def __init__(self):
        self.base_url = "https://api.pump.fun/v1"
        self.session = None
    
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    async def get_token_info(self, token_address):
        """Get detailed token information from pump.fun"""
        endpoint = f"{self.base_url}/tokens/{token_address}"
        try:
            async with self.session.get(endpoint) as response:
                if response.status == 200:
                    data = await response.json()
                    return {
                        'name': data.get('name'),
                        'symbol': data.get('symbol'),
                        'market_cap': float(data.get('marketCap', 0)),
                        'price': float(data.get('price', 0)),
                        'holders': int(data.get('holders', 0)),
                        'total_supply': float(data.get('totalSupply', 0))
                    }
                return None
        except Exception as e:
            logging.error(f"Error fetching token info: {e}")
            return None
    
    async def get_token_holders(self, token_address):
        """Get detailed holder information"""
        endpoint = f"{self.base_url}/tokens/{token_address}/holders"
        try:
            async with self.session.get(endpoint) as response:
                if response.status == 200:
                    data = await response.json()
                    holders = []
                    total_supply = float(data.get('totalSupply', 0))
                    
                    for holder in data.get('holders', []):
                        balance = float(holder.get('balance', 0))
                        percentage = (balance / total_supply * 100) if total_supply > 0 else 0
                        holders.append({
                            'address': holder.get('address'),
                            'balance': balance,
                            'percentage': percentage,
                            'is_contract': holder.get('isContract', False)
                        })
                    
                    return holders
                return []
        except Exception as e:
            logging.error(f"Error fetching holder info: {e}")
            return []
    
    async def get_token_trades(self, token_address, limit=100):
        """Get recent trades for a token"""
        endpoint = f"{self.base_url}/tokens/{token_address}/trades"
        params = {'limit': limit}
        
        try:
            async with self.session.get(endpoint, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    return [{
                        'timestamp': trade.get('timestamp'),
                        'type': trade.get('type'),  # 'buy' or 'sell'
                        'amount': float(trade.get('amount', 0)),
                        'price': float(trade.get('price', 0)),
                        'trader': trade.get('trader')
                    } for trade in data.get('trades', [])]
                return []
        except Exception as e:
            logging.error(f"Error fetching trade info: {e}")
            return []
    
    async def monitor_new_tokens(self):
        """Monitor for new token launches on pump.fun"""
        endpoint = f"{self.base_url}/tokens/new"
        
        while True:
            try:
                async with self.session.get(endpoint) as response:
                    if response.status == 200:
                        tokens = await response.json()
                        for token in tokens:
                            yield {
                                'address': token.get('address'),
                                'name': token.get('name'),
                                'symbol': token.get('symbol'),
                                'deployer': token.get('deployer'),
                                'launch_time': token.get('launchTime'),
                                'initial_market_cap': float(token.get('initialMarketCap', 0))
                            }
                    
                await asyncio.sleep(5)  # Poll every 5 seconds
                    
            except Exception as e:
                logging.error(f"Error monitoring new tokens: {e}")
                await asyncio.sleep(30)  # Longer delay on error

async def test_pump_api():
    """Test the Bitquery Pump Fun API integration."""
    api = PumpAPI()
    
    # Test with BONK token
    test_token = "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263"
    
    print(f"\nGetting recent trades for: {test_token}")
    trades = await api.get_token_trades(test_token, limit=5)
    if trades:
        print("\nRecent Trades:")
        for trade in trades:
            print(json.dumps(trade, indent=2))
    else:
        print("No trades found or error occurred")
    
    print(f"\nGetting token stats for: {test_token}")
    stats = await api.get_token_stats(test_token)
    if stats:
        print("\nToken Stats:")
        print(json.dumps(stats, indent=2))
    else:
        print("No stats found or error occurred")

async def test_token_subscription():
    """Test the new token subscription functionality."""
    api = PumpAPI()
    
    def handle_new_token(token: TokenCreation):
        print("\nNew Token Created:")
        print(json.dumps(token, indent=2))
    
    print("Listening for new token creations... (Press Ctrl+C to stop)")
    try:
        await api.subscribe_to_new_tokens(handle_new_token)
    except KeyboardInterrupt:
        print("\nStopping subscription...")
        await api.stop_subscription()
    except Exception as e:
        print(f"Error: {e}")
        await api.stop_subscription()

async def test_pumpfun_api():
    async with PumpFunAPI() as api:
        test_token = "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263"
        print(f"\nGetting token info for: {test_token}")
        info = await api.get_token_info(test_token)
        if info:
            print("\nToken Info:")
            print(json.dumps(info, indent=2))
        else:
            print("No info found or error occurred")
        
        print(f"\nGetting token holders for: {test_token}")
        holders = await api.get_token_holders(test_token)
        if holders:
            print("\nToken Holders:")
            for holder in holders:
                print(json.dumps(holder, indent=2))
        else:
            print("No holders found or error occurred")
        
        print(f"\nGetting token trades for: {test_token}")
        trades = await api.get_token_trades(test_token, limit=5)
        if trades:
            print("\nToken Trades:")
            for trade in trades:
                print(json.dumps(trade, indent=2))
        else:
            print("No trades found or error occurred")
        
        print("\nMonitoring new tokens...")
        async for token in api.monitor_new_tokens():
            print("\nNew Token:")
            print(json.dumps(token, indent=2))

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    asyncio.run(test_pump_api())
    asyncio.run(test_token_subscription())
    asyncio.run(test_pumpfun_api())
