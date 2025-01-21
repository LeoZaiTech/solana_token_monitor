import asyncio
import websockets
import os
from dotenv import load_dotenv

load_dotenv()
HELIUS_API_KEY = os.getenv("HELIUS_API_KEY")
url = f"wss://ws.helius-rpc.com/?api-key={HELIUS_API_KEY}"

async def test_connection():
    try:
        async with websockets.connect(url) as ws:
            print("Connected successfully!")
            await ws.send('{"jsonrpc":"2.0","id":1,"method":"ping","params":[]}')
            response = await ws.recv()
            print("Response:", response)
    except Exception as e:
        print(f"Connection failed: {e}")

asyncio.run(test_connection())
