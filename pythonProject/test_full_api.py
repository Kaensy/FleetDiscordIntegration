import asyncio
import os
from dotenv import load_dotenv
from src.oauth.client import BoltOAuthClient
from src.api.bolt_client import BoltFleetClient

load_dotenv()


async def test_full_api():
    """Test the complete API integration"""

    oauth_client = BoltOAuthClient(
        client_id=os.getenv('BOLT_CLIENT_ID'),
        client_secret=os.getenv('BOLT_CLIENT_SECRET')
    )

    # Use the company ID we found
    fleet_client = BoltFleetClient(oauth_client, company_id=172774)

    try:
        async with fleet_client:
            print("Testing Fleet Info...")
            fleet_info = await fleet_client.get_fleet_info()
            print(f"Fleet Info: {fleet_info}\n")

            print("Testing Recent Orders...")
            orders = await fleet_client.get_trip_data()
            print(f"Found {len(orders)} recent orders\n")

            print("Testing Earnings...")
            earnings = await fleet_client.get_earnings_data()
            print(f"Earnings: {earnings}\n")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        await oauth_client.close()


if __name__ == "__main__":
    asyncio.run(test_full_api())