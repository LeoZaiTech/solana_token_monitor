import asyncio
import websockets
import os
from dotenv import load_dotenv
import json

# Load environment variables
load_dotenv()
HELIUS_API_KEY = os.getenv("HELIUS_API_KEY")

async def test_websocket():
    url = f"wss://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
    try:
        async with websockets.connect(url) as websocket:
            print("Connected successfully")

            # Subscription request to monitor a specific token account
            request = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "accountSubscribe",
                "params": [
                    "CM78CPUeXjn8o3yroDHxUtKsZZgoy4GPKPPXfouKNH12",  # Replace with your token account
                    {
                        "encoding": "jsonParsed",
                        "commitment": "finalized"
                    }
                ]
            }

            await websocket.send(json.dumps(request))
            response = await websocket.recv()
            print("Response:", response)

    except Exception as e:
        print(f"WebSocket connection failed: {e}")

asyncio.run(test_websocket())
