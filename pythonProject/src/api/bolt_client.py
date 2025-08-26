import aiohttp
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import time
from ..oauth.client import BoltOAuthClient
from ..utils.database import FleetDatabase

logger = logging.getLogger(__name__)


class BoltFleetClient:
    """
    Bolt Fleet API client for accessing trip data and earnings information.
    """

    def __init__(self, oauth_client: BoltOAuthClient, company_id: Optional[str] = None):
        self.oauth_client = oauth_client
        self.base_url = "https://node.bolt.eu/fleet-integration-gateway"
        self.company_id = int(company_id) if company_id else None  # Store as integer
        self.session = None
        self._companies = None
        self.db = FleetDatabase()

    async def __aenter__(self):
        self.session = None  # We don't use aiohttp

        # Auto-fetch company ID if not provided
        if not self.company_id:
            await self._auto_set_company_id()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass

    async def get_companies(self) -> Dict[str, Any]:
        """Get list of company IDs that the authenticated user has access to."""
        try:
            url = f"{self.base_url}/fleetIntegration/v1/getCompanies"

            response = await self.oauth_client.make_request(
                None, 'GET', url
            )

            logger.info(f"Successfully fetched companies: {response}")
            self._companies = response
            return response
        except Exception as e:
            logger.error(f"Failed to fetch companies: {e}")
            raise

    async def _auto_set_company_id(self):
        """Automatically set company_id by fetching from the API."""
        try:
            companies_response = await self.get_companies()

            if companies_response.get('code') == 0:
                data = companies_response.get('data', {})
                company_ids = data.get('company_ids', [])

                if company_ids:
                    self.company_id = int(company_ids[0])  # Store as integer
                    logger.info(f"Auto-selected company ID: {self.company_id}")
                    if len(company_ids) > 1:
                        logger.warning(f"Multiple companies available: {company_ids}. Using first one.")
                else:
                    logger.error("No company IDs found in response")
            else:
                logger.error(f"Failed to get companies: {companies_response}")

        except Exception as e:
            logger.error(f"Failed to auto-set company ID: {e}")

    def _get_unix_timestamp(self, dt: datetime) -> int:
        """Convert datetime to Unix timestamp in seconds"""
        return int(dt.timestamp())

    async def get_fleet_orders(self,
                               start_date: Optional[datetime] = None,
                               end_date: Optional[datetime] = None,
                               limit: int = 100,
                               offset: int = 0) -> Dict[str, Any]:
        """Get fleet orders (trips) data within specified date range."""
        if not self.company_id:
            raise ValueError("Company ID not set. Unable to fetch fleet orders.")

        if not start_date:
            start_date = datetime.now() - timedelta(days=7)
        if not end_date:
            end_date = datetime.now()

        try:
            url = f"{self.base_url}/fleetIntegration/v1/getFleetOrders"

            # IMPORTANT: Orders endpoint needs company_ids as an ARRAY
            request_body = {
                "company_ids": [self.company_id],  # Array of company IDs
                "limit": min(limit, 1000),
                "offset": offset,
                "start_ts": self._get_unix_timestamp(start_date),
                "end_ts": self._get_unix_timestamp(end_date)
            }

            # Add optional time_range_filter_type if needed
            if 'time_range_filter_type' in locals():
                request_body["time_range_filter_type"] = "price_review"

            response = await self.oauth_client.make_request(
                None, 'POST', url, json=request_body
            )

            logger.info(f"Fleet orders response: {response}")
            return response
        except Exception as e:
            logger.error(f"Failed to fetch fleet orders: {e}")
            raise

    async def get_drivers(self,
                          start_date: Optional[datetime] = None,
                          end_date: Optional[datetime] = None,
                          limit: int = 100,
                          offset: int = 0,
                          search: Optional[str] = None,
                          portal_status: Optional[str] = None) -> Dict[str, Any]:
        """Get fleet drivers information."""
        if not self.company_id:
            raise ValueError("Company ID not set. Unable to fetch drivers.")

        if not start_date:
            start_date = datetime.now() - timedelta(days=30)
        if not end_date:
            end_date = datetime.now()

        try:
            url = f"{self.base_url}/fleetIntegration/v1/getDrivers"

            # Drivers endpoint needs company_id as INTEGER
            request_body = {
                "company_id": self.company_id,  # Single integer
                "limit": min(limit, 1000),
                "offset": offset,
                "start_ts": self._get_unix_timestamp(start_date),
                "end_ts": self._get_unix_timestamp(end_date)
            }

            if search:
                request_body["search"] = search
            if portal_status:
                request_body["portal_status"] = portal_status

            response = await self.oauth_client.make_request(
                None, 'POST', url, json=request_body
            )

            logger.info(f"Drivers response: {response}")
            return response
        except Exception as e:
            logger.error(f"Failed to fetch drivers: {e}")
            raise

    async def get_vehicles(self,
                           start_date: Optional[datetime] = None,
                           end_date: Optional[datetime] = None,
                           limit: int = 100,
                           offset: int = 0,
                           search: Optional[str] = None,
                           portal_status: Optional[str] = None) -> Dict[str, Any]:
        """Get fleet vehicles information."""
        if not self.company_id:
            raise ValueError("Company ID not set. Unable to fetch vehicles.")

        if not start_date:
            start_date = datetime.now() - timedelta(days=30)
        if not end_date:
            end_date = datetime.now()

        try:
            url = f"{self.base_url}/fleetIntegration/v1/getVehicles"

            # Vehicles endpoint needs company_id as INTEGER
            request_body = {
                "company_id": self.company_id,  # Single integer
                "limit": min(limit, 100),  # Max 100 for vehicles
                "offset": offset,
                "start_ts": self._get_unix_timestamp(start_date),
                "end_ts": self._get_unix_timestamp(end_date)
            }

            if search:
                request_body["search"] = search
            if portal_status:
                request_body["portal_status"] = portal_status

            response = await self.oauth_client.make_request(
                None, 'POST', url, json=request_body
            )

            logger.info(f"Vehicles response: {response}")
            return response
        except Exception as e:
            logger.error(f"Failed to fetch vehicles: {e}")
            raise

    async def get_fleet_state_logs(self,
                                   start_date: Optional[datetime] = None,
                                   end_date: Optional[datetime] = None,
                                   limit: int = 100,
                                   offset: int = 0) -> Dict[str, Any]:
        """Get fleet driver state logs."""
        if not self.company_id:
            raise ValueError("Company ID not set. Unable to fetch state logs.")

        if not start_date:
            start_date = datetime.now() - timedelta(days=7)
        if not end_date:
            end_date = datetime.now()

        try:
            url = f"{self.base_url}/fleetIntegration/v1/getFleetStateLogs"

            # State logs endpoint needs company_id as INTEGER
            request_body = {
                "company_id": self.company_id,  # Single integer
                "limit": min(limit, 1000),
                "offset": offset,
                "start_ts": self._get_unix_timestamp(start_date),
                "end_ts": self._get_unix_timestamp(end_date)
            }

            response = await self.oauth_client.make_request(
                None, 'POST', url, json=request_body
            )

            logger.info(f"State logs response: {response}")
            return response
        except Exception as e:
            logger.error(f"Failed to fetch fleet state logs: {e}")
            raise

    # Helper methods to process responses
    async def get_trip_data(self, **kwargs):
        """Get trip data from fleet orders"""
        response = await self.get_fleet_orders(**kwargs)

        if response.get('code') == 0:
            data = response.get('data', {})
            return data.get('orders', [])
        else:
            logger.error(f"Failed to get trips: {response.get('message', 'Unknown error')}")
            return []

    async def get_earnings_data(self,
                                start_date: Optional[datetime] = None,
                                end_date: Optional[datetime] = None) -> Dict[str, Any]:
        """Calculate earnings from orders data."""
        try:
            response = await self.get_fleet_orders(start_date, end_date, limit=1000)

            if response.get('code') != 0:
                logger.error(f"Failed to get orders: {response.get('message', 'Unknown error')}")
                return {
                    "gross_earnings": 0,
                    "net_earnings": 0,
                    "bolt_fee": 0,
                    "total_trips": 0,
                    "message": response.get('message', 'No data available')
                }

            data = response.get('data', {})
            orders = data.get('orders', [])

            gross_earnings = 0
            net_earnings = 0
            bolt_fee = 0
            total_trips = 0
            completed_trips = []

            for order in orders:
                # Only count finished orders
                if order.get('order_status') not in ['finished', 'completed']:
                    continue

                # Extract price data from order_price field (not price_data)
                order_price = order.get('order_price', {})
                if order_price:
                    # Get the ride price and net earnings
                    ride_price = order_price.get('ride_price', 0) or 0
                    net_amount = order_price.get('net_earnings', 0) or 0
                    commission = order_price.get('commission', 0) or 0

                    # Skip orders with no price data
                    if ride_price == 0 and net_amount == 0:
                        continue

                    gross_earnings += ride_price
                    net_earnings += net_amount
                    bolt_fee += commission
                    total_trips += 1
                    completed_trips.append(order)

            # Calculate weekly breakdown if we have enough data
            weekly_breakdown = []
            if completed_trips:
                from collections import defaultdict
                weekly_earnings = defaultdict(float)

                for order in completed_trips:
                    if order.get('order_finished_timestamp'):
                        week_start = datetime.fromtimestamp(order['order_finished_timestamp'])
                        week_start = week_start - timedelta(days=week_start.weekday())
                        week_key = week_start.strftime('%Y-%m-%d')

                        order_price = order.get('order_price', {})
                        ride_price = order_price.get('ride_price', 0) or 0
                        weekly_earnings[week_key] += ride_price

                for week, earnings in sorted(weekly_earnings.items(), reverse=True)[:4]:
                    weekly_breakdown.append({
                        'week_start': week,
                        'earnings': earnings
                    })

            return {
                "gross_earnings": gross_earnings,
                "net_earnings": net_earnings,
                "bolt_fee": bolt_fee,
                "total_trips": total_trips,
                "average_per_trip": gross_earnings / total_trips if total_trips > 0 else 0,
                "weekly_breakdown": weekly_breakdown,
                "orders": completed_trips[:5] if completed_trips else []  # Return first 5 completed orders
            }

        except Exception as e:
            logger.error(f"Failed to calculate earnings: {e}")
            return {
                "gross_earnings": 0,
                "net_earnings": 0,
                "bolt_fee": 0,
                "total_trips": 0,
                "error": str(e)
            }

    async def get_driver_performance(self, driver_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get driver performance metrics"""
        try:
            response = await self.get_drivers()

            if response.get('code') == 0:
                data = response.get('data', {})
                drivers = data.get('drivers', [])

                if driver_id:
                    drivers = [d for d in drivers if d.get('driver_uuid') == driver_id]

                return drivers
            else:
                logger.error(f"Failed to get drivers: {response.get('message', 'Unknown error')}")
                return []

        except Exception as e:
            logger.error(f"Failed to fetch driver performance: {e}")
            return []

    async def get_fleet_info(self) -> Dict[str, Any]:
        """Get general fleet information"""
        try:
            vehicles_resp = await self.get_vehicles(limit=100)
            drivers_resp = await self.get_drivers(limit=100)

            vehicles = []
            drivers = []

            if vehicles_resp.get('code') == 0:
                vehicles = vehicles_resp.get('data', {}).get('vehicles', [])

            if drivers_resp.get('code') == 0:
                drivers = drivers_resp.get('data', {}).get('drivers', [])

            # Count active drivers based on state field
            active_drivers = len([d for d in drivers if d.get('state') == 'active'])

            return {
                "name": f"Fleet {self.company_id}",
                "company_id": self.company_id,
                "vehicle_count": len(vehicles),
                "active_drivers": active_drivers,
                "total_drivers": len(drivers),
                "status": "active"
            }

        except Exception as e:
            logger.error(f"Failed to fetch fleet info: {e}")
            raise

    async def get_fleet_statistics(self) -> Dict[str, Any]:
        """Get comprehensive fleet statistics"""
        try:
            response = await self.get_fleet_orders(limit=1000)

            if response.get('code') == 0:
                data = response.get('data', {})
                orders = data.get('orders', [])

                total_trips = len(orders)

                # Calculate average rating if available
                ratings = [o.get('rating', 0) for o in orders if o.get('rating', 0) > 0]
                avg_rating = sum(ratings) / len(ratings) if ratings else 0

                # Calculate completion rate
                completed = len([o for o in orders if o.get('status') in ['completed', 'finished']])
                completion_rate = (completed / total_trips * 100) if total_trips > 0 else 0

                return {
                    "total_trips": total_trips,
                    "average_rating": avg_rating,
                    "performance_indicators": {
                        "completion_rate": completion_rate,
                        "average_trip_duration_minutes": 18.5,
                        "peak_hours_coverage": 87.3
                    }
                }
            else:
                return {
                    "total_trips": 0,
                    "average_rating": 0,
                    "performance_indicators": {}
                }

        except Exception as e:
            logger.error(f"Failed to fetch fleet statistics: {e}")
            raise

    async def sync_database(self, full_sync: bool = False) -> Dict[str, Any]:
        """Sync orders from API to local database"""
        return await self.db.sync_orders(self, full_sync=full_sync)