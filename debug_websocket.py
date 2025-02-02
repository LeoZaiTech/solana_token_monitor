import os
import json
import asyncio
import websockets
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
load_dotenv()
HELIUS_API_KEY = os.getenv("HELIUS_API_KEY")
PUMP_FUN_PROGRAM_ID = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"

async def debug_websocket():
    """Debug WebSocket messages"""
    print("\n🔍 Starting WebSocket Debug Session")
    
    ws_url = f"wss://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
    print(f"🔗 Connecting to Helius...")
    
    try:
        async with websockets.connect(ws_url) as websocket:
            print("✅ Connected!")
            
            # Subscribe to pump.fun program
            subscribe_msg = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "programSubscribe",
                "params": [
                    PUMP_FUN_PROGRAM_ID,
                    {
                        "encoding": "jsonParsed",
                        "commitment": "confirmed",
                        "filters": [
                            {
                                "memcmp": {
                                    "offset": 0,
                                    "bytes": "82a2124e4f31"  # Token creation discriminator
                                }
                            }
                        ]
                    }
                ]
            }
            
            print("📡 Subscribing to program...")
            await websocket.send(json.dumps(subscribe_msg))
            print("✅ Subscription sent")
            
            print("\n📥 Waiting for messages...")
            print("Press Ctrl+C to stop\n")
            
            while True:
                try:
                    message = await websocket.recv()
                    data = json.loads(message)
                    
                    # Pretty print the message
                    print(f"\n⏰ {datetime.now().strftime('%H:%M:%S')} - New Message:")
                    print("=" * 50)
                    print(json.dumps(data, indent=2))
                    print("=" * 50)
                    
                except json.JSONDecodeError:
                    print("❌ Invalid JSON received")
                    continue
                    
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    try:
        asyncio.run(debug_websocket())
    except KeyboardInterrupt:
        print("\n\n👋 Debug session ended by user")
