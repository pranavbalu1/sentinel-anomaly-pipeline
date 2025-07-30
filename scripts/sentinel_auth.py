import os
import requests
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Read from environment
CLIENT_ID = os.getenv("SENTINEL_CLIENT_ID")
CLIENT_SECRET = os.getenv("SENTINEL_CLIENT_SECRET")

def get_access_token():
    if not CLIENT_ID or not CLIENT_SECRET:
        raise EnvironmentError("❌ Missing CLIENT_ID or CLIENT_SECRET in environment variables.")

    url = "https://services.sentinel-hub.com/oauth/token"
    payload = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET
    }

    response = requests.post(url, data=payload)
    if response.status_code == 200:
        access_token = response.json()["access_token"]
        print("✅ Access Token retrieved.")
        print(f"Access Token: {access_token[:10]}...")
        return access_token
    else:
        raise Exception(f"Token request failed: {response.status_code} - {response.text}")

# Test
if __name__ == "__main__":
    token = get_access_token()