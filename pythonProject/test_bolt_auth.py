import os
import requests
from dotenv import load_dotenv

load_dotenv()


def test_bolt_auth():
    """Test Bolt API authentication using requests library"""

    client_id = os.getenv('BOLT_CLIENT_ID')
    client_secret = os.getenv('BOLT_CLIENT_SECRET')

    print(f"Client ID: {client_id[:10]}..." if client_id else "NOT SET")
    print(f"Client Secret: {client_secret[:10]}..." if client_secret else "NOT SET")

    url = "https://oidc.bolt.eu/token"

    # Exact data format as CURL
    data = {
        'client_id': client_id,
        'client_secret': client_secret,
        'grant_type': 'client_credentials',
        'scope': 'fleet-integration:api'
    }

    # Headers that exactly match CURL
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'User-Agent': 'curl/7.68.0',
        'Accept': '*/*'
    }

    print(f"\nTesting authentication to: {url}")
    print("Using requests library with CURL emulation")

    try:
        # Make the request
        response = requests.post(url, data=data, headers=headers, timeout=10)

        print(f"\nResponse Status: {response.status_code}")

        if response.status_code == 200:
            json_data = response.json()
            print("\n✅ SUCCESS! Token received:")
            print(f"  Access Token: {json_data.get('access_token', 'N/A')[:20]}...")
            print(f"  Expires In: {json_data.get('expires_in', 'N/A')} seconds")
            print(f"  Token Type: {json_data.get('token_type', 'N/A')}")
            return json_data
        else:
            print(f"\n❌ FAILED with status {response.status_code}")
            print(f"Response: {response.text[:500]}")

    except Exception as e:
        print(f"\n❌ Error: {e}")


if __name__ == "__main__":
    test_bolt_auth()