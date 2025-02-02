import os
import json
import asyncio
import aiohttp
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
load_dotenv()
HELIUS_API_KEY = os.getenv("HELIUS_API_KEY")

async def analyze_transaction(signature: str):
    """Analyze a specific transaction"""
    print(f"\n🔍 Analyzing transaction: {signature}")
    
    url = f"https://api.helius.xyz/v0/transactions/?api-key={HELIUS_API_KEY}"
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params={"commitment": "confirmed", "signatures": [signature]}) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    if not data:
                        print("❌ No transaction data found")
                        return
                        
                    tx = data[0]
                    
                    print("\n📊 Transaction Details:")
                    print(f"Block Time: {datetime.fromtimestamp(tx.get('timestamp', 0))}")
                    print(f"Status: {tx.get('status', 'unknown')}")
                    
                    print("\n🔄 Instructions:")
                    for idx, instruction in enumerate(tx.get('instructions', [])):
                        print(f"\nInstruction {idx + 1}:")
                        print(f"Program: {instruction.get('programId', 'unknown')}")
                        print(f"Data (hex): {instruction.get('data', 'none')}")
                        
                        # Check if this is a token creation
                        if instruction.get('data', '').startswith('82a2124e4f31'):
                            print("✨ This is a token creation instruction!")
                    
                    print("\n📝 Account Keys:")
                    for idx, account in enumerate(tx.get('accountKeys', [])):
                        print(f"{idx + 1}. {account}")
                    
                else:
                    print(f"❌ API request failed: {response.status}")
                    
    except Exception as e:
        print(f"❌ Error: {e}")

# Example transaction to analyze
EXAMPLE_TX = "LC48hmk1dyBcrtbKTbyW55ia4N1vmjcYsdf6NhoKvvHfPnvh5Ce49QnJNGYQ54L1Pxg4T4Yag65mmPvEvN7CW5H"

if __name__ == "__main__":
    asyncio.run(analyze_transaction(EXAMPLE_TX))
