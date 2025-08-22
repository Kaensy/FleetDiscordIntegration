import asyncio
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
from src.oauth.client import BoltOAuthClient

load_dotenv()


async def test_orders_debug():
    """Debug the orders endpoint to find the correct request format"""

    oauth_client = BoltOAuthClient(
        client_id=os.getenv('BOLT_CLIENT_ID'),
        client_secret=os.getenv('BOLT_CLIENT_SECRET')
    )

    url = "https://node.bolt.eu/fleet-integration-gateway/fleetIntegration/v1/getFleetOrders"

    # Test different request body formats
    test_cases = [
        {
            "name": "Test 1: Basic with company_id as string",
            "body": {
                "company_id": "172774",
                "limit": 10,
                "offset": 0
            }
        },
        {
            "name": "Test 2: Basic with company_id as integer",
            "body": {
                "company_id": 172774,
                "limit": 10,
                "offset": 0
            }
        },
        {
            "name": "Test 3: With timestamps (seconds)",
            "body": {
                "company_id": 172774,
                "limit": 10,
                "offset": 0,
                "start_ts": int((datetime.now() - timedelta(days=7)).timestamp()),
                "end_ts": int(datetime.now().timestamp())
            }
        },
        {
            "name": "Test 4: With timestamps (milliseconds)",
            "body": {
                "company_id": 172774,
                "limit": 10,
                "offset": 0,
                "start_ts": int((datetime.now() - timedelta(days=7)).timestamp() * 1000),
                "end_ts": int(datetime.now().timestamp() * 1000)
            }
        },
        {
            "name": "Test 5: With time_range_filter_type",
            "body": {
                "company_id": 172774,
                "limit": 10,
                "offset": 0,
                "start_ts": int((datetime.now() - timedelta(days=7)).timestamp()),
                "end_ts": int(datetime.now().timestamp()),
                "time_range_filter_type": "price_review"
            }
        },
        {
            "name": "Test 6: With company_ids array",
            "body": {
                "company_ids": [172774],
                "limit": 10,
                "offset": 0
            }
        },
        {
            "name": "Test 7: Minimal - just company_id",
            "body": {
                "company_id": 172774
            }
        }
    ]

    for test in test_cases:
        print(f"\n{'=' * 60}")
        print(f"{test['name']}")
        print(f"Request body: {test['body']}")
        print(f"{'=' * 60}")

        try:
            response = await oauth_client.make_request(
                None, 'POST', url, json=test['body']
            )

            print(f"✅ Response: {response}")

            # If successful, show some data
            if response.get('code') == 0:
                data = response.get('data', {})
                orders = data.get('orders', [])
                print(f"Found {len(orders)} orders")
                if orders:
                    print(f"First order: {orders[0]}")
                break  # Stop testing once we find a working format
            else:
                print(f"⚠️ Error: Code {response.get('code')}, Message: {response.get('message')}")

        except Exception as e:
            print(f"❌ Exception: {e}")

    await oauth_client.close()


if __name__ == "__main__":
    asyncio.run(test_orders_debug())