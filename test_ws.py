import asyncio
import websockets
import json
import os
from dotenv import load_dotenv

load_dotenv()

HELIUS_API_KEY = os.getenv('HELIUS_API_KEY')

async def test_websocket():
    ws_url = f"wss://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
    print(f"Connecting to {ws_url}")
    
    try:
        async with websockets.connect(ws_url) as websocket:
            # Simple account subscribe
            subscribe_msg = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "accountSubscribe",
                "params": [
                    "7i5KKsX2weiTkry7jA4ZwSuXGhs5eJBEjY8vVxR4pfRx",
                    {"encoding": "jsonParsed"}
                ]
            }
            
            print("Sending subscription message...")
            await websocket.send(json.dumps(subscribe_msg))
            
            print("Waiting for response...")
            response = await websocket.recv()
            print(f"Received: {response}")
            
            # Keep connection alive
            while True:
                msg = await websocket.recv()
                print(f"Update received: {msg}")
                
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(test_websocket())
