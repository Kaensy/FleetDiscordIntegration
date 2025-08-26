# Save this as src/utils/database.py (REPLACE the existing one)

import sqlite3
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple
from pathlib import Path

logger = logging.getLogger(__name__)


class FleetDatabase:
    """
    Enhanced database with hours worked and cash collection tracking
    """

    def __init__(self, db_path: str = "fleet_data.db"):
        self.db_path = Path(db_path)
        self.init_database()

    def init_database(self):
        """Initialize database with proper indexes"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            # Main orders table - the source of truth
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS orders (
                    order_reference TEXT PRIMARY KEY,
                    driver_uuid TEXT NOT NULL,
                    driver_name TEXT,
                    order_status TEXT,
                    ride_distance INTEGER,
                    ride_price REAL,
                    net_earnings REAL,
                    commission REAL,
                    order_created_timestamp INTEGER,
                    order_finished_timestamp INTEGER,
                    order_accepted_timestamp INTEGER,
                    pickup_lat REAL,
                    pickup_lng REAL,
                    dropoff_lat REAL,
                    dropoff_lng REAL,
                    vehicle_plate TEXT,
                    payment_method TEXT,
                    rating INTEGER,
                    synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # Sync tracking table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS sync_status (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    last_sync_timestamp INTEGER,
                    orders_synced INTEGER,
                    sync_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # Create indexes for performance
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_orders_finished ON orders(order_finished_timestamp)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_orders_driver ON orders(driver_uuid)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(order_status)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_orders_payment ON orders(payment_method)')

            conn.commit()
            logger.info("Database initialized successfully")

    async def sync_orders(self, bolt_client, full_sync: bool = False) -> Dict[str, Any]:
        """Smart sync: Only fetch orders newer than our last sync"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            # Get last sync point
            if not full_sync:
                cursor.execute('''
                    SELECT MAX(order_created_timestamp) 
                    FROM orders
                ''')
                last_timestamp = cursor.fetchone()[0]

                if last_timestamp:
                    # Add 1 second to avoid duplicates
                    start_date = datetime.fromtimestamp(last_timestamp + 1)
                    logger.info(f"Incremental sync: orders after {start_date}")
                else:
                    full_sync = True

            if full_sync:
                # Full sync - get last 30 days
                start_date = datetime.now() - timedelta(days=30)
                logger.info("Full sync: fetching last 30 days")

            # Fetch from API
            new_orders = 0
            updated_orders = 0
            offset = 0

            while True:
                try:
                    response = await bolt_client.get_fleet_orders(
                        start_date=start_date,
                        end_date=datetime.now(),
                        limit=1000,
                        offset=offset
                    )

                    if response.get('code') != 0:
                        break

                    orders = response.get('data', {}).get('orders', [])
                    if not orders:
                        break

                    # Store orders
                    for order in orders:
                        if self._store_order(conn, order):
                            new_orders += 1
                        else:
                            updated_orders += 1

                    offset += len(orders)

                    # Safety limit for full sync
                    if full_sync and offset >= 10000:
                        logger.warning("Reached 10,000 orders limit in full sync")
                        break

                    logger.info(f"Processed {offset} orders...")

                except Exception as e:
                    logger.error(f"Error during sync: {e}")
                    break

            conn.commit()

            # Update sync status
            cursor.execute('''
                INSERT INTO sync_status (last_sync_timestamp, orders_synced)
                VALUES (?, ?)
            ''', (int(datetime.now().timestamp()), new_orders))
            conn.commit()

            logger.info(f"Sync complete: {new_orders} new, {updated_orders} updated")

            return {
                'new_orders': new_orders,
                'updated_orders': updated_orders,
                'total_processed': new_orders + updated_orders,
                'status': 'success'
            }

    def _store_order(self, conn, order: Dict) -> bool:
        """Store a single order"""
        try:
            cursor = conn.cursor()

            # Extract coordinates
            pickup_lat = pickup_lng = dropoff_lat = dropoff_lng = None
            for stop in order.get('order_stops', []):
                if stop.get('type') == 'pickup':
                    pickup_lat = stop.get('real_lat') or stop.get('lat')
                    pickup_lng = stop.get('real_lng') or stop.get('lng')
                elif stop.get('type') == 'dropoff':
                    dropoff_lat = stop.get('real_lat') or stop.get('lat')
                    dropoff_lng = stop.get('real_lng') or stop.get('lng')

            order_price = order.get('order_price', {}) or {}

            cursor.execute('''
                INSERT OR REPLACE INTO orders (
                    order_reference, driver_uuid, driver_name, order_status,
                    ride_distance, ride_price, net_earnings, commission,
                    order_created_timestamp, order_finished_timestamp, order_accepted_timestamp,
                    pickup_lat, pickup_lng, dropoff_lat, dropoff_lng,
                    vehicle_plate, payment_method, rating
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                order.get('order_reference'),
                order.get('driver_uuid'),
                order.get('driver_name'),
                order.get('order_status'),
                order.get('ride_distance'),
                order_price.get('ride_price'),
                order_price.get('net_earnings'),
                order_price.get('commission'),
                order.get('order_created_timestamp'),
                order.get('order_finished_timestamp'),
                order.get('order_accepted_timestamp'),
                pickup_lat, pickup_lng, dropoff_lat, dropoff_lng,
                order.get('vehicle_license_plate'),
                order.get('payment_method'),
                order.get('rating')
            ))

            return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Failed to store order: {e}")
            return False

    def get_driver_daily_stats(self, date: datetime) -> List[Dict[str, Any]]:
        """Get daily statistics for each driver based on order finished timestamp"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            # Calculate start and end of day timestamps
            start_of_day = datetime(date.year, date.month, date.day)
            end_of_day = start_of_day + timedelta(days=1)
            start_ts = int(start_of_day.timestamp())
            end_ts = int(end_of_day.timestamp())

            # Get stats grouped by driver, using order_finished_timestamp for date filtering
            cursor.execute('''
                SELECT 
                    driver_uuid,
                    driver_name,
                    COUNT(*) as orders_completed,
                    COALESCE(SUM(ride_price), 0) as gross_earnings,
                    COALESCE(SUM(net_earnings), 0) as net_earnings,
                    COALESCE(SUM(ride_distance) / 1000.0, 0) as kms_traveled,
                    COALESCE(SUM(CASE WHEN payment_method = 'cash' THEN net_earnings ELSE 0 END), 0) as cash_collected
                FROM orders
                WHERE order_finished_timestamp >= ? 
                AND order_finished_timestamp < ?
                AND order_status = 'finished'
                GROUP BY driver_uuid
            ''', (start_ts, end_ts))

            results = []
            driver_data = cursor.fetchall()

            for row in driver_data:
                driver_uuid = row[0]

                # Calculate hours worked for this driver on this day
                cursor.execute('''
                    SELECT 
                        MIN(order_accepted_timestamp) as first_order,
                        MAX(order_finished_timestamp) as last_order
                    FROM orders
                    WHERE driver_uuid = ?
                    AND order_finished_timestamp >= ? 
                    AND order_finished_timestamp < ?
                    AND order_status = 'finished'
                    AND order_accepted_timestamp IS NOT NULL
                ''', (driver_uuid, start_ts, end_ts))

                time_row = cursor.fetchone()
                hours_worked = 0
                if time_row and time_row[0] and time_row[1]:
                    hours_worked = (time_row[1] - time_row[0]) / 3600

                earnings_per_hour = row[3] / hours_worked if hours_worked > 0 else 0

                results.append({
                    'driver_uuid': row[0],
                    'driver_name': row[1],
                    'orders_completed': row[2],
                    'gross_earnings': round(row[3], 2),
                    'net_earnings': round(row[4], 2),
                    'kms_traveled': round(row[5], 1),
                    'cash_collected': round(row[6], 2),
                    'hours_worked': round(hours_worked, 1),
                    'earnings_per_hour': round(earnings_per_hour, 2)
                })

            return results

    def get_fleet_stats(self, days: Optional[int] = None) -> Dict[str, Any]:
        """Get fleet statistics for specified days or all time"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            # Build query based on days parameter
            if days:
                start_ts = int((datetime.now() - timedelta(days=days)).timestamp())
                time_filter = "WHERE order_finished_timestamp >= ? AND order_status = 'finished'"
                params = (start_ts,)
            else:
                time_filter = "WHERE order_status = 'finished'"
                params = ()

            cursor.execute(f'''
                SELECT 
                    COUNT(*) as total_trips,
                    COALESCE(SUM(ride_distance) / 1000.0, 0) as total_distance_km
                FROM orders
                {time_filter}
            ''', params)

            stats = cursor.fetchone()

            return {
                'total_trips': stats[0] or 0,
                'total_distance_km': round(stats[1], 1)
            }

    def get_all_drivers(self) -> List[Tuple[int, str]]:
        """Get list of all drivers with their names"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            cursor.execute('''
                SELECT DISTINCT driver_uuid, driver_name
                FROM orders
                WHERE driver_name IS NOT NULL
                ORDER BY driver_name
            ''')

            return [(idx + 1, row[0], row[1]) for idx, row in enumerate(cursor.fetchall())]

    def calculate_online_hours_from_states(self, driver_uuid: str, start_date: datetime, end_date: datetime,
                                           state_logs: List[Dict]) -> float:
        """Calculate actual online hours from state logs"""
        # Filter logs for this driver
        driver_logs = [log for log in state_logs if log.get('driver_uuid') == driver_uuid]

        if not driver_logs:
            return 0.0

        # Sort by timestamp
        driver_logs.sort(key=lambda x: x.get('created', 0))

        total_online_seconds = 0
        last_active_time = None

        for log in driver_logs:
            timestamp = log.get('created')
            state = log.get('state', '').lower()

            if state in ['active', 'online']:
                last_active_time = timestamp
            elif state in ['inactive', 'offline'] and last_active_time:
                # Calculate duration of this active period
                if timestamp > last_active_time:
                    total_online_seconds += (timestamp - last_active_time)
                last_active_time = None

        # If still active at end of period, count time until end
        if last_active_time and end_date:
            end_ts = int(end_date.timestamp())
            if end_ts > last_active_time:
                total_online_seconds += (end_ts - last_active_time)

        return total_online_seconds / 3600  # Convert to hours

    def get_driver_stats_by_uuid(self, driver_uuid: str, days: Optional[int] = None,
                                 state_logs: Optional[List[Dict]] = None) -> Dict[str, Any]:
        """Get detailed driver statistics"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            # Build query based on days parameter
            if days:
                end_date = datetime.now()
                start_date = end_date - timedelta(days=days)
                start_ts = int(start_date.timestamp())
                end_ts = int(end_date.timestamp())
                time_filter = "AND order_finished_timestamp >= ? AND order_finished_timestamp < ?"
                params = (driver_uuid, start_ts, end_ts)
                date_range = f"{start_date.strftime('%b %d')} - {end_date.strftime('%b %d')}"
            else:
                time_filter = ""
                params = (driver_uuid,)
                date_range = "All Time"

            # Get main stats
            cursor.execute(f'''
                SELECT 
                    driver_name,
                    COUNT(*) as orders_completed,
                    COALESCE(SUM(ride_price), 0) as gross_earnings,
                    COALESCE(SUM(net_earnings), 0) as net_earnings,
                    COALESCE(SUM(ride_distance) / 1000.0, 0) as total_distance,
                    COALESCE(SUM(CASE WHEN payment_method = 'cash' THEN (ride_price - commission) ELSE 0 END), 0) as cash_collected
                FROM orders
                WHERE driver_uuid = ? 
                AND order_status = 'finished'
                {time_filter}
            ''', params)

            row = cursor.fetchone()
            if not row or row[1] == 0:  # No orders found
                return None

            # Calculate hours worked - if state logs provided, use them for accurate online time
            if state_logs:
                hours_worked = self.calculate_online_hours_from_states(
                    driver_uuid,
                    datetime.fromtimestamp(start_ts) if days else datetime.fromtimestamp(0),
                    datetime.fromtimestamp(end_ts) if days else datetime.now(),
                    state_logs
                )
            else:
                # Fallback to day-by-day calculation from orders
                cursor.execute(f'''
                    SELECT 
                        DATE(order_finished_timestamp, 'unixepoch') as work_date,
                        MIN(order_accepted_timestamp) as daily_start,
                        MAX(order_finished_timestamp) as daily_end
                    FROM orders
                    WHERE driver_uuid = ? 
                    AND order_status = 'finished'
                    AND order_accepted_timestamp IS NOT NULL
                    {time_filter}
                    GROUP BY work_date
                ''', params)

                hours_worked = 0
                for day_row in cursor.fetchall():
                    if day_row[1] and day_row[2]:
                        daily_hours = (day_row[2] - day_row[1]) / 3600
                        hours_worked += daily_hours

            # Calculate derived metrics
            earnings_per_hour = row[2] / hours_worked if hours_worked > 0 else 0
            earnings_per_km = row[2] / row[4] if row[4] > 0 else 0
            avg_distance = row[4] / row[1] if row[1] > 0 else 0

            return {
                'driver_name': row[0],
                'orders_completed': row[1],
                'gross_earnings': round(row[2], 2),
                'net_earnings': round(row[3], 2),
                'total_distance': round(row[4], 1),
                'hours_worked': round(hours_worked, 1),
                'earnings_per_hour': round(earnings_per_hour, 2),
                'earnings_per_km': round(earnings_per_km, 2),
                'avg_distance': round(avg_distance, 1),
                'cash_collected': round(row[5], 2),
                'date_range': date_range
            }

    def get_company_earnings(self, days: Optional[int] = None) -> Dict[str, Any]:
        """Get company-wide earnings statistics with date range"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            # Build query based on days parameter
            if days:
                end_date = datetime.now()
                start_date = end_date - timedelta(days=days)
                start_ts = int(start_date.timestamp())
                end_ts = int(end_date.timestamp())
                time_filter = "WHERE order_finished_timestamp >= ? AND order_finished_timestamp < ? AND order_status = 'finished'"
                params = (start_ts, end_ts)
                date_range = f"{start_date.strftime('%b %d')} - {end_date.strftime('%b %d')}"
            else:
                time_filter = "WHERE order_status = 'finished'"
                params = ()
                date_range = "All Time"

            cursor.execute(f'''
                SELECT 
                    COUNT(*) as trips_completed,
                    COALESCE(SUM(ride_price), 0) as gross_earnings,
                    COALESCE(SUM(net_earnings), 0) as net_earnings,
                    COALESCE(SUM(ride_distance) / 1000.0, 0) as total_distance
                FROM orders
                {time_filter}
            ''', params)

            row = cursor.fetchone()

            # Calculate derived metrics
            earnings_per_trip = row[1] / row[0] if row[0] > 0 else 0
            earnings_per_km = row[1] / row[3] if row[3] > 0 else 0

            return {
                'trips_completed': row[0] or 0,
                'gross_earnings': round(row[1], 2),
                'net_earnings': round(row[2], 2),
                'earnings_per_trip': round(earnings_per_trip, 2),
                'earnings_per_km': round(earnings_per_km, 2),
                'total_distance': round(row[3], 1),
                'date_range': date_range
            }

    def get_database_stats(self) -> Dict[str, Any]:
        """Get database statistics"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            cursor.execute('SELECT COUNT(*) FROM orders')
            total_orders = cursor.fetchone()[0]

            cursor.execute('''
                SELECT 
                    MIN(order_created_timestamp),
                    MAX(order_created_timestamp)
                FROM orders
            ''')
            date_range = cursor.fetchone()

            # Database file size
            db_size_mb = self.db_path.stat().st_size / (1024 * 1024) if self.db_path.exists() else 0

            return {
                'total_orders': total_orders,
                'database_size_mb': round(db_size_mb, 2),
                'date_range': {
                    'start': datetime.fromtimestamp(date_range[0]).isoformat() if date_range[0] else None,
                    'end': datetime.fromtimestamp(date_range[1]).isoformat() if date_range[1] else None
                }
            }