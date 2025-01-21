import os
import requests
from dotenv import load_dotenv

load_dotenv()
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")

payload = {"content": "Test notification from Python script"}
headers = {"Content-Type": "application/json"}

response = requests.post(DISCORD_WEBHOOK_URL, json=payload, headers=headers)
print(f"Discord Response: {response.status_code} - {response.text}")
