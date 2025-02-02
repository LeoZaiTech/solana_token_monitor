import os
from dotenv import load_dotenv
import aiohttp
import asyncio
from datetime import datetime

# Load environment variables
load_dotenv()
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")

async def test_discord_notification():
    print("\n🚀 Testing Discord Notifications")
    
    if not DISCORD_WEBHOOK_URL:
        print("❌ Discord webhook URL not found!")
        return
        
    print("📤 Sending test notification...")
    
    try:
        embed = {
            "title": "Token Monitor Test Alert!",
            "description": (
                "**Test Notification**\n"
                f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}\n"
                "If you see this message, Discord notifications are working!"
            ),
            "color": 65280  # Green color
        }
        
        payload = {
            "embeds": [embed]
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.post(DISCORD_WEBHOOK_URL, json=payload) as response:
                if response.status == 204:
                    print("✅ Discord notification sent successfully!")
                else:
                    print(f"❌ Failed to send notification. Status: {response.status}")
                    
    except Exception as e:
        print(f"❌ Error sending notification: {e}")

async def main():
    await test_discord_notification()

if __name__ == "__main__":
    asyncio.run(main())
