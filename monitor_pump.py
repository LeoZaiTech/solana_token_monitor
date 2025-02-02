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

async def notify_discord(message):
    """Send a notification to Discord"""
    if not DISCORD_WEBHOOK_URL:
        return
        
    try:
        embed = {
            "title": "Pump.fun Token Alert!",
            "description": message,
            "color": 65280  # Green
        }
        
        async with aiohttp.ClientSession() as session:
            await session.post(DISCORD_WEBHOOK_URL, json={"embeds": [embed]})
            
    except Exception as e:
        print(f"❌ Discord error: {e}")

async def monitor_pump_fun():
    """Monitor pump.fun program for new tokens"""
    print("\n🚀 Starting Pump.fun Monitor")
    
    ws_url = f"wss://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
    
    while True:
        try:
            print("\n🔌 Connecting to WebSocket...")
            async with websockets.connect(ws_url) as websocket:
                print("✅ Connected!")
                
                # Subscribe to program
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
                
                print("📡 Subscribing to program...")
                await websocket.send(json.dumps(subscribe_msg))
                
                # Track statistics
                messages_received = 0
                program_notifications = 0
                
                print("\n📊 Monitoring Statistics:")
                while True:
                    message = await websocket.recv()
                    messages_received += 1
                    
                    try:
                        data = json.loads(message)
                        
                        if 'method' in data and data['method'] == 'programNotification':
                            program_notifications += 1
                            
                            # Get the notification details
                            result = data['params']['result']
                            
                            # Print detailed info
                            print(f"\n🔔 Program Notification {program_notifications}:")
                            print(f"Type: {result.get('type', 'unknown')}")
                            if 'value' in result:
                                print("Data:", json.dumps(result['value'], indent=2))
                            
                            # Notify Discord
                            await notify_discord(f"New program activity detected!\n```{json.dumps(result, indent=2)}```")
                        
                        # Update stats
                        print(f"\r📈 Messages: {messages_received} | Notifications: {program_notifications} | Last: {datetime.now().strftime('%H:%M:%S')}", end='')
                        
                    except json.JSONDecodeError:
                        print("\n❌ Invalid JSON received")
                    except Exception as e:
                        print(f"\n❌ Error processing message: {e}")
                        
        except Exception as e:
            print(f"\n❌ WebSocket error: {e}")
            print("🔄 Reconnecting in 5 seconds...")
            await asyncio.sleep(5)

if __name__ == "__main__":
    try:
        asyncio.run(monitor_pump_fun())
    except KeyboardInterrupt:
        print("\n\n👋 Monitor stopped by user")
