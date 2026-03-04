"""Authentication helpers for obtaining Sentinel Hub OAuth tokens."""

import os

import requests
from dotenv import load_dotenv

# Load environment variables from `.env` before reading credentials.
load_dotenv()

# Sentinel Hub OAuth client credentials (must exist in environment).
CLIENT_ID = os.getenv("SENTINEL_CLIENT_ID")
CLIENT_SECRET = os.getenv("SENTINEL_CLIENT_SECRET")


def get_access_token() -> str:
    """Request and return a Sentinel Hub bearer token using client credentials."""
    if not CLIENT_ID or not CLIENT_SECRET:
        raise EnvironmentError("❌ Missing CLIENT_ID or CLIENT_SECRET in environment variables.")

    url = "https://services.sentinel-hub.com/oauth/token"
    payload = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
    }

    # Exchange credentials for an access token.
    response = requests.post(url, data=payload)
    if response.status_code == 200:
        access_token = response.json()["access_token"]
        print("✅ Access Token retrieved.")
        print(f"Access Token: {access_token[:10]}...")
        return access_token

    # Bubble up API response details for debugging auth setup issues.
    raise Exception(f"Token request failed: {response.status_code} - {response.text}")


if __name__ == "__main__":
    # Manual smoke test for local credential validation.
    token = get_access_token()
