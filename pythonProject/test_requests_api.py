import asyncio
import os
from dotenv import load_dotenv
from src.oauth.client import BoltOAuthClient

load_dotenv()


async def test_api_with_requests():
    """Test API using requests library"""

    oauth_client = BoltOAuthClient(
        client_id=os.getenv('BOLT_CLIENT_ID'),
        client_secret=os.getenv('BOLT_CLIENT_SECRET')
    )

    try:
        # Test getting companies
        print("Testing getCompanies endpoint...")
        url = "https://node.bolt.eu/fleet-integration-gateway/fleetIntegration/v1/getCompanies"

        # We pass None for session since we don't use it
        response = await oauth_client.make_request(None, 'GET', url)

        print(f"✅ Success! Response: {response}")

        # Extract company IDs
        if response.get('code') == 0:
            data = response.get('data', {})
            company_ids = data.get('company_ids', [])
            print(f"Found company IDs: {company_ids}")
        else:
            print(f"Response: {response}")

    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        await oauth_client.close()


if __name__ == "__main__":
    asyncio.run(test_api_with_requests())