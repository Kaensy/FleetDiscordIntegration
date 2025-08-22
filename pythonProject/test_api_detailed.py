import asyncio
import aiohttp
import os
from dotenv import load_dotenv
from src.oauth.client import BoltOAuthClient

load_dotenv()


async def test_companies_detailed():
    """Test companies endpoint with detailed debugging"""

    # Initialize OAuth client
    oauth_client = BoltOAuthClient(
        client_id=os.getenv('BOLT_CLIENT_ID'),
        client_secret=os.getenv('BOLT_CLIENT_SECRET')
    )

    # Get a valid token
    token = await oauth_client.get_valid_token()
    print(f"✅ Got token: {token[:20]}...")

    url = "https://node.bolt.eu/fleet-integration-gateway/fleetIntegration/v1/getCompanies"

    # Try different header combinations
    header_sets = [
        {
            'Authorization': f'Bearer {token}',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        },
        {
            'Authorization': f'Bearer {token}',
            'Accept': 'application/json'
        },
        {
            'Authorization': f'Bearer {token}',
            'Accept': '*/*'
        }
    ]

    async with aiohttp.ClientSession() as session:
        for i, headers in enumerate(header_sets, 1):
            print(f"\n{'=' * 60}")
            print(f"Attempt {i} with headers: {list(headers.keys())}")
            print(f"{'=' * 60}")

            try:
                async with session.get(url, headers=headers) as response:
                    print(f"Status: {response.status}")
                    print(f"Headers: {dict(response.headers)}")

                    # Get the response text regardless of status
                    text = await response.text()
                    print(f"Response body: {text}")

                    # Try to parse as JSON if possible
                    try:
                        import json
                        data = json.loads(text)
                        print(f"Parsed JSON: {json.dumps(data, indent=2)}")

                        # Check if it's actually a success with a NOT_AUTHORIZED message
                        if data.get('code') == 503:
                            print(
                                "\n⚠️ Got code 503 (NOT_AUTHORIZED) - This might mean you need to be granted access to use the API")
                        elif data.get('message') == 'NOT_AUTHORIZED':
                            print("\n⚠️ NOT_AUTHORIZED message - Your credentials might not have fleet access")

                    except json.JSONDecodeError:
                        print("Response is not JSON")

            except Exception as e:
                print(f"Error: {e}")

    await oauth_client.close()


if __name__ == "__main__":
    asyncio.run(test_companies_detailed())