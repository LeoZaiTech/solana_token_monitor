import os
import json
import asyncio
import websockets
from dotenv import load_dotenv
from datetime import datetime
import aiohttp

# Load environment variables
load_dotenv()
HELIUS_API_KEY = os.getenv("HELIUS_API_KEY")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")
PUMP_FUN_PROGRAM_ID = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"

async def test_websocket():
    """Test WebSocket connection and subscription"""
    print("\n🔌 Testing WebSocket Connection")
    
    ws_url = f"wss://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
    print(f"🔗 Connecting to: wss://mainnet.helius-rpc.com/?api-key=****{HELIUS_API_KEY[-4:]}")
    
    try:
        async with websockets.connect(ws_url) as websocket:
            print("✅ WebSocket connected!")
            
            # Subscribe to pump.fun program
            subscribe_msg = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "programSubscribe",
                "params": [
                    PUMP_FUN_PROGRAM_ID,
                    {
                        "encoding": "jsonParsed",
                        "commitment": "confirmed"
                    }
                ]
            }
            
            print("📡 Sending subscription message...")
            await websocket.send(json.dumps(subscribe_msg))
            print("✅ Subscription message sent")
            
            print("\n🔍 Waiting for messages (will timeout after 30 seconds)...")
            message_count = 0
            
            try:
                while True:
                    message = await asyncio.wait_for(websocket.recv(), timeout=30)
                    message_count += 1
                    data = json.loads(message)
                    
                    print(f"\n📥 Message {message_count} received!")
                    print(f"Type: {data.get('method', 'unknown')}")
                    
                    if message_count >= 5:  # Stop after 5 messages
                        break
                        
            except asyncio.TimeoutError:
                print("⏰ No messages received for 30 seconds")
                
            print(f"\n📊 Total messages received: {message_count}")
            
    except Exception as e:
        print(f"❌ WebSocket error: {e}")

async def test_discord():
    """Test Discord webhook"""
    print("\n🔔 Testing Discord Notifications")
    
    if not DISCORD_WEBHOOK_URL:
        print("❌ Discord webhook URL not configured!")
        return
        
    try:
        embed = {
            "title": "Token Monitor Test",
            "description": f"Test message sent at {datetime.now().strftime('%H:%M:%S')}",
            "color": 65280
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.post(DISCORD_WEBHOOK_URL, json={"embeds": [embed]}) as response:
                if response.status == 204:
                    print("✅ Discord notification sent successfully!")
                else:
                    print(f"❌ Discord notification failed (status: {response.status})")
                    
    except Exception as e:
        print(f"❌ Discord error: {e}")

async def main():
    print("\n🚀 Starting Component Tests\n")
    
    # Test Discord first
    await test_discord()
    
    # Then test WebSocket
    await test_websocket()
    
    print("\n✨ Tests completed!")

if __name__ == "__main__":
    asyncio.run(main())
