import aiohttp
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
from ..oauth.client import BoltOAuthClient

logger = logging.getLogger(__name__)


class BoltFleetClient:
    """
    Bolt Fleet API client for accessing trip data and earnings information.
    """

    def __init__(self, oauth_client: BoltOAuthClient):
        self.oauth_client = oauth_client
        self.base_url = "https://fleets.bolt.eu/api"  # Base API URL
        self.session = None

    async def __aenter__(self):
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30),
            connector=aiohttp.TCPConnector(limit=100, limit_per_host=30)
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def get_fleet_info(self) -> Dict[str, Any]:
        """Get general fleet information"""
        try:
            url = f"{self.base_url}/v1/fleet/info"
            response = await self.oauth_client.make_request(
                self.session, 'GET', url
            )
            logger.info("Successfully fetched fleet info")
            return response
        except Exception as e:
            logger.error(f"Failed to fetch fleet info: {e}")
            raise

    async def get_trip_data(self,
                            start_date: Optional[datetime] = None,
                            end_date: Optional[datetime] = None,
                            limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get trip data within specified date range.

        Args:
            start_date: Start date for trip data (default: 7 days ago)
            end_date: End date for trip data (default: now)
            limit: Maximum number of trips to return
        """
        if not start_date:
            start_date = datetime.now() - timedelta(days=7)
        if not end_date:
            end_date = datetime.now()

        try:
            url = f"{self.base_url}/v1/trips"
            params = {
                'start_date': start_date.isoformat(),
                'end_date': end_date.isoformat(),
                'limit': limit
            }

            response = await self.oauth_client.make_request(
                self.session, 'GET', url, params=params
            )

            trips = response.get('trips', [])
            logger.info(f"Successfully fetched {len(trips)} trips")
            return trips
        except Exception as e:
            logger.error(f"Failed to fetch trip data: {e}")
            raise

    async def get_earnings_data(self,
                                start_date: Optional[datetime] = None,
                                end_date: Optional[datetime] = None) -> Dict[str, Any]:
        """
        Get earnings information within specified date range.

        Args:
            start_date: Start date for earnings data (default: 30 days ago)
            end_date: End date for earnings data (default: now)
        """
        if not start_date:
            start_date = datetime.now() - timedelta(days=30)
        if not end_date:
            end_date = datetime.now()

        try:
            url = f"{self.base_url}/v1/earnings"
            params = {
                'start_date': start_date.isoformat(),
                'end_date': end_date.isoformat()
            }

            response = await self.oauth_client.make_request(
                self.session, 'GET', url, params=params
            )

            logger.info("Successfully fetched earnings data")
            return response
        except Exception as e:
            logger.error(f"Failed to fetch earnings data: {e}")
            raise

    async def get_driver_performance(self, driver_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get driver performance metrics"""
        try:
            url = f"{self.base_url}/v1/drivers/performance"
            params = {}
            if driver_id:
                params['driver_id'] = driver_id

            response = await self.oauth_client.make_request(
                self.session, 'GET', url, params=params
            )

            drivers = response.get('drivers', [])
            logger.info(f"Successfully fetched performance data for {len(drivers)} drivers")
            return drivers
        except Exception as e:
            logger.error(f"Failed to fetch driver performance: {e}")
            raise

    async def get_fleet_statistics(self) -> Dict[str, Any]:
        """Get comprehensive fleet statistics"""
        try:
            url = f"{self.base_url}/v1/fleet/statistics"
            response = await self.oauth_client.make_request(
                self.session, 'GET', url
            )

            logger.info("Successfully fetched fleet statistics")
            return response
        except Exception as e:
            logger.error(f"Failed to fetch fleet statistics: {e}")
            raise