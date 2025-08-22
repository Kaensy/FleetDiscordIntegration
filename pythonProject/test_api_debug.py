import asyncio
import aiohttp
import os
from dotenv import load_dotenv
from src.oauth.client import BoltOAuthClient

load_dotenv()


async def debug_api_calls():
    """Debug API calls to see what's being returned"""

    # Initialize OAuth client
    oauth_client = BoltOAuthClient(
        client_id=os.getenv('BOLT_CLIENT_ID'),
        client_secret=os.getenv('BOLT_CLIENT_SECRET')
    )

    # Get a valid token
    token = await oauth_client.get_valid_token()
    print(f"✅ Got token: {token[:20]}...")

    # Test different URL variations
    test_urls = [
        "https://fleets.bolt.eu/fleetIntegration/v1/getCompanies",
        "https://api.bolt.eu/fleetIntegration/v1/getCompanies",
        "https://fleet-api.bolt.eu/v1/getCompanies",
        "https://partner.bolt.eu/fleetIntegration/v1/getCompanies",
    ]

    headers = {
        'Authorization': f'Bearer {token}',
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'User-Agent': 'BoltFleetAPI/1.0'
    }

    async with aiohttp.ClientSession() as session:
        for url in test_urls:
            print(f"\n{'=' * 60}")
            print(f"Testing: {url}")
            print(f"{'=' * 60}")

            try:
                async with session.get(url, headers=headers, timeout=10) as response:
                    print(f"Status: {response.status}")
                    print(f"Content-Type: {response.headers.get('Content-Type', 'None')}")

                    # Get the raw text response
                    text = await response.text()

                    if 'application/json' in response.headers.get('Content-Type', ''):
                        print(f"✅ JSON Response: {text[:500]}")
                    else:
                        print(f"⚠️ Non-JSON Response (first 500 chars):")
                        print(text[:500])

                        # Check if it's a redirect or login page
                        if 'login' in text.lower() or 'signin' in text.lower():
                            print("\n🔴 Appears to be a login page - authentication might not be working")
                        if 'cloudflare' in text.lower():
                            print("\n🔴 Cloudflare protection detected")

            except asyncio.TimeoutError:
                print(f"❌ Timeout")
            except Exception as e:
                print(f"❌ Error: {e}")

    # Now test if we need to use a different base URL from documentation
    print(f"\n{'=' * 60}")
    print("Testing with potential API gateway URL")
    print(f"{'=' * 60}")

    # Try the companies endpoint with different approaches
    async with aiohttp.ClientSession() as session:
        # Test if maybe it needs to be accessed through an API gateway
        potential_urls = [
            "https://fleet.api.bolt.eu/v1/companies",
            "https://fleets.bolt.eu/api/v1/companies",
            "https://fleets.bolt.eu/integration/v1/companies",
        ]

        for url in potential_urls:
            print(f"\nTrying: {url}")
            try:
                async with session.get(url, headers=headers, timeout=5) as response:
                    print(f"  Status: {response.status}")
                    if response.status == 200:
                        text = await response.text()
                        print(f"  Response preview: {text[:200]}")
            except Exception as e:
                print(f"  Error: {str(e)[:100]}")

    await oauth_client.close()


if __name__ == "__main__":
    asyncio.run(debug_api_calls())