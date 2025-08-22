import asyncio
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
from src.oauth.client import BoltOAuthClient

load_dotenv()


async def test_drivers_debug():
    """Debug the drivers endpoint"""

    oauth_client = BoltOAuthClient(
        client_id=os.getenv('BOLT_CLIENT_ID'),
        client_secret=os.getenv('BOLT_CLIENT_SECRET')
    )

    url = "https://node.bolt.eu/fleet-integration-gateway/fleetIntegration/v1/getDrivers"

    # Test different request formats
    test_cases = [
        {
            "name": "Test 1: Just company_id",
            "body": {
                "company_id": 172774
            }
        },
        {
            "name": "Test 2: With limit and offset",
            "body": {
                "company_id": 172774,
                "limit": 10,
                "offset": 0
            }
        },
        {
            "name": "Test 3: With timestamps",
            "body": {
                "company_id": 172774,
                "limit": 10,
                "offset": 0,
                "start_ts": int((datetime.now() - timedelta(days=30)).timestamp()),
                "end_ts": int(datetime.now().timestamp())
            }
        }
    ]

    for test in test_cases:
        print(f"\n{test['name']}")
        print(f"Body: {test['body']}")

        try:
            response = await oauth_client.make_request(
                None, 'POST', url, json=test['body']
            )

            if response.get('code') == 0:
                print(f"✅ Success! Found {len(response.get('data', {}).get('drivers', []))} drivers")
                drivers = response.get('data', {}).get('drivers', [])
                if drivers:
                    print(f"First driver: {drivers[0]}")
                break
            else:
                print(f"⚠️ Error: {response.get('message')}")

        except Exception as e:
            print(f"❌ Exception: {e}")

    await oauth_client.close()


if __name__ == "__main__":
    asyncio.run(test_drivers_debug())