import asyncio
import os
from dotenv import load_dotenv
from src.oauth.client import BoltOAuthClient
from src.api.bolt_client import BoltFleetClient

load_dotenv()


async def test_get_companies():
    """Test fetching company IDs"""

    # Initialize OAuth client
    oauth_client = BoltOAuthClient(
        client_id=os.getenv('BOLT_CLIENT_ID'),
        client_secret=os.getenv('BOLT_CLIENT_SECRET')
    )

    # Initialize Fleet client (without company_id)
    fleet_client = BoltFleetClient(oauth_client)

    try:
        async with fleet_client:
            # Get companies
            companies = await fleet_client.get_companies()

            print("Companies Response:")
            print(companies)

            # Auto-set should have happened
            print(f"\nAuto-selected Company ID: {fleet_client.company_id}")

            # Now test fetching some data
            if fleet_client.company_id:
                print("\nTesting fleet info with auto-selected company ID...")
                fleet_info = await fleet_client.get_fleet_info()
                print(f"Fleet Info: {fleet_info}")

    finally:
        await oauth_client.close()


if __name__ == "__main__":
    asyncio.run(test_get_companies())