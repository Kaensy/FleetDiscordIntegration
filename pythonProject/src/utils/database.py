import sqlite3
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple
from pathlib import Path

logger = logging.getLogger(__name__)


class FleetDatabase:
    """
    Enhanced database with corrected hours worked and cash collection tracking
    """

    def __init__(self, db_path: str = None):
        if db_path is None:
            # Use data directory for database storage
            data_dir = Path("data")
            data_dir.mkdir(exist_ok=True)
            db_path = data_dir / "fleet_data.db"
        self.db_path = Path(db_path)
        self.init_database()

    def init_database(self):
        """Initialize database with proper indexes and all required columns"""
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
                    order_pickup_timestamp INTEGER,
                    order_drop_off_timestamp INTEGER,
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

            # Auto-migrate existing database if columns are missing
            self._ensure_columns_exist(cursor)

            conn.commit()
            logger.info("Database initialized successfully")

    def _ensure_columns_exist(self, cursor):
        """Ensure all required columns exist, add them if missing"""
        # Get existing columns
        cursor.execute("PRAGMA table_info(orders)")
        existing_columns = [column[1] for column in cursor.fetchall()]

        # Define required columns that might be missing
        required_columns = [
            ('order_pickup_timestamp', 'INTEGER'),
            ('order_drop_off_timestamp', 'INTEGER'),
        ]

        # Add missing columns
        for column_name, column_type in required_columns:
            if column_name not in existing_columns:
                try:
                    cursor.execute(f'ALTER TABLE orders ADD COLUMN {column_name} {column_type}')
                    logger.info(f"Added missing column: {column_name}")
                except Exception as e:
                    logger.error(f"Failed to add column {column_name}: {e}")

    def _get_available_columns(self, conn) -> List[str]:
        """Get list of available columns in the orders table"""
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(orders)")
        return [column[1] for column in cursor.fetchall()]

    def _build_order_times_query(self, available_columns: List[str]) -> str:
        """Build query for order times based on available columns"""
        base_columns = [
            'order_accepted_timestamp',
            'order_finished_timestamp'
        ]

        optional_columns = []
        if 'order_pickup_timestamp' in available_columns:
            optional_columns.append('order_pickup_timestamp')
        else:
            optional_columns.append('NULL as order_pickup_timestamp')

        if 'order_drop_off_timestamp' in available_columns:
            optional_columns.append('order_drop_off_timestamp')
        else:
            optional_columns.append('NULL as order_drop_off_timestamp')

        all_columns = base_columns + optional_columns
        return ', '.join(all_columns)

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
        """Store a single order with all available timestamps"""
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
                    order_pickup_timestamp, order_drop_off_timestamp,
                    pickup_lat, pickup_lng, dropoff_lat, dropoff_lng,
                    vehicle_plate, payment_method, rating
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                order.get('order_pickup_timestamp'),  # NEW
                order.get('order_drop_off_timestamp'),  # NEW
                pickup_lat, pickup_lng, dropoff_lat, dropoff_lng,
                order.get('vehicle_license_plate'),
                order.get('payment_method'),
                order.get('rating')
            ))

            return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Failed to store order: {e}")
            return False

    def _calculate_working_hours_from_orders(self, order_times: List[Tuple]) -> float:
        """
        Calculate ACTIVE working hours (time engaged with customers) - SAFE VERSION
        This should match what Bolt website shows as "Active online time"
        """
        if not order_times:
            return 0.0

        total_active_seconds = 0

        # For each order, calculate the time from acceptance to completion
        # This represents active/engaged time with customers
        for order_data in order_times:
            # Handle different tuple lengths safely
            accepted = order_data[0] if len(order_data) > 0 else None
            finished = order_data[1] if len(order_data) > 1 else None
            pickup = order_data[2] if len(order_data) > 2 else None
            dropoff = order_data[3] if len(order_data) > 3 else None

            if not accepted or not finished:
                continue

            # Active time = from order acceptance to order completion
            # This includes: driving to pickup, waiting, picking up, driving to destination, dropping off
            order_active_time = finished - accepted

            # Sanity check: cap individual orders at 2 hours (120 minutes)
            # Most rides shouldn't take more than 2 hours
            order_active_time = min(order_active_time, 2 * 3600)

            total_active_seconds += order_active_time

            # Debug logging
            order_duration_minutes = order_active_time / 60
            logger.debug(f"Order active time: {order_duration_minutes:.1f} minutes")

        total_active_hours = total_active_seconds / 3600
        logger.info(f"Total ACTIVE hours calculated: {total_active_hours:.2f}")

        return total_active_hours

    def calculate_online_hours_from_states(self, driver_uuid: str, start_date: datetime, end_date: datetime,
                                           state_logs: List[Dict]) -> float:
        """Calculate actual online hours from state logs - IMPROVED VERSION"""

        # Filter logs for this driver within the date range
        start_ts = int(start_date.timestamp()) if start_date else 0
        end_ts = int(end_date.timestamp()) if end_date else int(datetime.now().timestamp())

        driver_logs = [
            log for log in state_logs
            if log.get('driver_uuid') == driver_uuid
               and start_ts <= log.get('created', 0) <= end_ts
        ]

        if not driver_logs:
            logger.warning(f"No state logs found for driver {driver_uuid} between {start_date} and {end_date}")
            return 0.0

        # Sort by timestamp
        driver_logs.sort(key=lambda x: x.get('created', 0))

        total_online_seconds = 0
        online_start = None

        for log in driver_logs:
            timestamp = log.get('created')
            state = log.get('state', '').lower()

            # Track when driver goes online/active
            if state in ['active', 'online', 'busy']:
                if online_start is None:
                    online_start = timestamp

            # Track when driver goes offline/inactive
            elif state in ['inactive', 'offline'] and online_start is not None:
                # Add this online period
                duration = timestamp - online_start
                if duration > 0:
                    total_online_seconds += duration
                online_start = None

        # If still online at the end of the period
        if online_start is not None:
            # Use the end of the period or current time
            end_timestamp = min(end_ts, int(datetime.now().timestamp()))
            if end_timestamp > online_start:
                total_online_seconds += (end_timestamp - online_start)

        hours = total_online_seconds / 3600
        logger.info(f"Driver {driver_uuid}: {len(driver_logs)} state logs, {hours:.2f} hours online")

        return round(hours, 2)

    def calculate_hours_from_state_logs(self, driver_uuid: str, start_date: datetime,
                                        end_date: datetime, state_logs: List[Dict]) -> float:
        """
        Calculate hours from Bolt's actual state transitions:
        - waiting_orders = online and available
        - has_order = on a trip
        - inactive = offline
        """
        # Filter logs for this driver
        driver_logs = [
            log for log in state_logs
            if log.get('driver_uuid') == driver_uuid
        ]

        if not driver_logs:
            logger.warning(f"No state logs found for driver {driver_uuid}")
            return 0.0

        # Sort by timestamp
        driver_logs.sort(key=lambda x: x.get('created', 0))

        # Calculate online time using Bolt's actual states
        total_online_seconds = 0
        online_start = None

        for log in driver_logs:
            state = log.get('state', '').lower()
            timestamp = log.get('created', 0)

            # Driver is online (waiting for orders or has an order)
            if state in ['waiting_orders', 'has_order']:
                if online_start is None:
                    online_start = timestamp

            # Driver goes offline (inactive)
            elif state == 'inactive' and online_start is not None:
                duration = timestamp - online_start
                if duration > 0:
                    total_online_seconds += duration
                online_start = None

        # If still online at the end of the period
        if online_start is not None:
            end_ts = int(end_date.timestamp()) if end_date else int(datetime.now().timestamp())
            if end_ts > online_start:
                duration = end_ts - online_start
                total_online_seconds += duration

        hours = total_online_seconds / 3600
        logger.info(f"Calculated {hours:.2f} active hours from state logs for driver {driver_uuid}")

        return round(hours, 2)

    def get_driver_stats_by_uuid(self, driver_uuid: str, days: Optional[int] = None,
                                 state_logs: Optional[List[Dict]] = None) -> Dict[str, Any]:
        """Get detailed driver statistics with accurate hours calculation"""
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
                start_date = datetime(2024, 7, 28)  # Company start date
                end_date = datetime.now()
                time_filter = ""
                params = (driver_uuid,)
                date_range = "All Time"

            # Get main stats with corrected cash calculation
            cursor.execute(f'''
                SELECT 
                    driver_name,
                    COUNT(*) as orders_completed,
                    COALESCE(SUM(ride_price), 0) as gross_earnings,
                    COALESCE(SUM(net_earnings), 0) as net_earnings,
                    COALESCE(SUM(ride_distance) / 1000.0, 0) as total_distance,
                    COALESCE(SUM(
                        CASE 
                            WHEN payment_method = 'cash' 
                            THEN ride_price - commission
                            ELSE 0 
                        END
                    ), 0) as cash_collected
                FROM orders
                WHERE driver_uuid = ? 
                AND order_status = 'finished'
                {time_filter}
            ''', params)

            row = cursor.fetchone()
            if not row or row[1] == 0:  # No orders found
                return None

            # Calculate active hours
            hours_worked = 0.0

            # Try to use state logs if provided
            if state_logs and len(state_logs) > 0:
                # Use the calculate_hours_from_state_logs method
                hours_worked = self.calculate_hours_from_state_logs(
                    driver_uuid,
                    start_date,
                    end_date,
                    state_logs
                )
                logger.info(f"Used state logs method: {hours_worked} hours")

            # Fallback: calculate from order timestamps if no hours from state logs
            if hours_worked == 0.0 and row[1] > 0:
                logger.info("Falling back to order-based calculation")

                # Get all orders for this period
                cursor.execute(f'''
                    SELECT 
                        order_accepted_timestamp,
                        order_finished_timestamp
                    FROM orders
                    WHERE driver_uuid = ?
                    AND order_status = 'finished'
                    AND order_accepted_timestamp IS NOT NULL
                    AND order_finished_timestamp IS NOT NULL
                    {time_filter}
                    ORDER BY order_accepted_timestamp
                ''', params)

                orders = cursor.fetchall()

                if orders:
                    # Sum up ride durations
                    total_seconds = 0
                    for accepted, finished in orders:
                        ride_duration = finished - accepted
                        total_seconds += ride_duration

                    # Add estimated waiting time between rides (5 minutes average)
                    if len(orders) > 1:
                        gap_time = (len(orders) - 1) * 5 * 60  # 5 minutes per gap in seconds
                        total_seconds += gap_time

                    hours_worked = round(total_seconds / 3600, 2)
                    logger.info(f"Order-based calculation: {hours_worked} hours from {len(orders)} orders")

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
                'hours_worked': hours_worked,
                'earnings_per_hour': round(earnings_per_hour, 2),
                'earnings_per_km': round(earnings_per_km, 2),
                'avg_distance': round(avg_distance, 1),
                'cash_collected': round(row[5], 2),
                'date_range': date_range
            }

    def get_driver_daily_stats(self, date: datetime, state_logs: Optional[List[Dict]] = None) -> List[Dict[str, Any]]:
        """Get daily statistics for each driver with accurate hours calculation"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            # Calculate start and end of day timestamps
            start_of_day = datetime(date.year, date.month, date.day)
            end_of_day = start_of_day + timedelta(days=1)
            start_ts = int(start_of_day.timestamp())
            end_ts = int(end_of_day.timestamp())

            # Get stats grouped by driver
            cursor.execute('''
                SELECT 
                    driver_uuid,
                    driver_name,
                    COUNT(*) as orders_completed,
                    COALESCE(SUM(ride_price), 0) as gross_earnings,
                    COALESCE(SUM(net_earnings), 0) as net_earnings,
                    COALESCE(SUM(ride_distance) / 1000.0, 0) as kms_traveled,
                    COALESCE(SUM(
                        CASE 
                            WHEN payment_method = 'cash' 
                            THEN ride_price - commission
                            ELSE 0 
                        END
                    ), 0) as cash_collected
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

                # Calculate active hours using the smart method
                hours_worked = self.calculate_active_hours(
                    driver_uuid,
                    start_of_day,
                    end_of_day,
                    state_logs
                )

                # If no hours but has orders, estimate
                if hours_worked == 0 and row[2] > 0:
                    hours_worked = round(row[2] * 0.33, 1)  # ~20 minutes per order

                earnings_per_hour = row[3] / hours_worked if hours_worked > 0 else 0

                results.append({
                    'driver_uuid': row[0],
                    'driver_name': row[1],
                    'orders_completed': row[2],
                    'gross_earnings': round(row[3], 2),
                    'net_earnings': round(row[4], 2),
                    'kms_traveled': round(row[5], 1),
                    'cash_collected': round(row[6], 2),
                    'hours_worked': hours_worked,
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

    def get_driver_stats_by_uuid(self, driver_uuid: str, days: Optional[int] = None,
                                 state_logs: Optional[List[Dict]] = None) -> Dict[str, Any]:
        """Get detailed driver statistics with ACTIVE time calculation (matches website) - SAFE VERSION"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            # Check what columns are available
            available_columns = self._get_available_columns(conn)
            logger.debug(f"Available database columns: {available_columns}")

            # FIXED DATE LOGIC: 1 day = today only, 3 days = today + 2 previous days
            if days:
                end_date = datetime.now()
                # For "today" (1 day), we want just today
                if days == 1:
                    # Today from 00:00:00 to 23:59:59
                    start_of_today = datetime(end_date.year, end_date.month, end_date.day, 0, 0, 0)
                    end_of_today = datetime(end_date.year, end_date.month, end_date.day, 23, 59, 59)
                    start_ts = int(start_of_today.timestamp())
                    end_ts = int(end_of_today.timestamp())
                    date_range = f"Today ({start_of_today.strftime('%b %d')})"
                else:
                    # For multiple days: today + (days-1) previous days
                    start_date = end_date - timedelta(days=days - 1)
                    start_of_period = datetime(start_date.year, start_date.month, start_date.day, 0, 0, 0)
                    end_of_today = datetime(end_date.year, end_date.month, end_date.day, 23, 59, 59)
                    start_ts = int(start_of_period.timestamp())
                    end_ts = int(end_of_today.timestamp())
                    date_range = f"Last {days} days ({start_of_period.strftime('%b %d')} - {end_of_today.strftime('%b %d')})"

                time_filter = "AND order_finished_timestamp >= ? AND order_finished_timestamp <= ?"
                params = (driver_uuid, start_ts, end_ts)
            else:
                time_filter = ""
                params = (driver_uuid,)
                date_range = "All Time"

            # Get main stats
            cursor.execute(f'''
                SELECT 
                    driver_name,
                    COUNT(*) as orders_completed,
                    COALESCE(SUM(CASE WHEN ride_price IS NOT NULL THEN ride_price ELSE 0 END), 0) as gross_earnings,
                    COALESCE(SUM(CASE WHEN net_earnings IS NOT NULL THEN net_earnings ELSE 0 END), 0) as net_earnings,
                    COALESCE(SUM(ride_distance) / 1000.0, 0) as total_distance,
                    COALESCE(SUM(CASE WHEN payment_method = 'cash' AND net_earnings IS NOT NULL THEN net_earnings ELSE 0 END), 0) as cash_collected
                FROM orders
                WHERE driver_uuid = ? 
                AND order_status = 'finished'
                {time_filter}
            ''', params)

            row = cursor.fetchone()
            if not row or row[1] == 0:  # No orders found
                return None

            # Get order times for ACTIVE hour calculation - SAFE QUERY
            order_columns = self._build_order_times_query(available_columns)

            cursor.execute(f'''
                SELECT {order_columns}
                FROM orders
                WHERE driver_uuid = ? 
                AND order_status = 'finished'
                AND order_accepted_timestamp IS NOT NULL
                AND order_finished_timestamp IS NOT NULL
                {time_filter}
                ORDER BY order_accepted_timestamp
            ''', params)

            order_times = cursor.fetchall()
            logger.info(f"Retrieved {len(order_times)} orders for active time calculation")

            # Calculate ACTIVE hours (time engaged with customers)
            # Priority: state logs for recent periods, order-based for longer periods
            if state_logs and days and days <= 30:
                hours_worked = self.calculate_online_hours_from_states(
                    driver_uuid,
                    datetime.fromtimestamp(start_ts) if days else datetime.fromtimestamp(0),
                    datetime.fromtimestamp(end_ts) if days else datetime.now(),
                    state_logs
                )
                logger.info(f"Using state logs for ACTIVE time: {hours_worked:.1f} hours for {row[0]}")
            else:
                # Use order-based ACTIVE time calculation
                hours_worked = self._calculate_working_hours_from_orders(order_times)
                logger.info(f"Using order-based ACTIVE time: {hours_worked:.1f} hours for {row[0]}")

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
                'hours_worked': round(hours_worked, 1),  # This is now ACTIVE time
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

    def calculate_hours_from_state_logs(self, driver_uuid: str, start_date: datetime, end_date: datetime,
                                        state_logs: List[Dict]) -> float:
        """
        Calculate hours from Bolt's actual state transitions:
        - waiting_orders = online and available
        - has_order = on a trip
        - inactive = offline
        """
        # Filter logs for this driver
        driver_logs = [
            log for log in state_logs
            if log.get('driver_uuid') == driver_uuid
        ]

        if not driver_logs:
            logger.warning(f"No state logs found for driver {driver_uuid}")
            return 0.0

        # Sort by timestamp
        driver_logs.sort(key=lambda x: x.get('created', 0))

        # Calculate online time using Bolt's actual states
        total_online_seconds = 0
        online_start = None

        for log in driver_logs:
            state = log.get('state', '').lower()
            timestamp = log.get('created', 0)

            # Driver goes online (waiting for orders or has an order)
            if state in ['waiting_orders', 'has_order']:
                if online_start is None:
                    online_start = timestamp
                    logger.debug(f"Driver went online at {datetime.fromtimestamp(timestamp)}")

            # Driver goes offline (inactive)
            elif state == 'inactive' and online_start is not None:
                duration = timestamp - online_start
                if duration > 0:
                    total_online_seconds += duration
                    logger.debug(
                        f"Driver went offline at {datetime.fromtimestamp(timestamp)}, duration: {duration / 3600:.2f} hrs")
                online_start = None

        # If still online at the end of the period
        if online_start is not None:
            end_ts = int(end_date.timestamp()) if end_date else int(datetime.now().timestamp())
            if end_ts > online_start:
                duration = end_ts - online_start
                total_online_seconds += duration
                logger.debug(f"Driver still online at end, adding {duration / 3600:.2f} hrs")

        hours = total_online_seconds / 3600
        logger.info(f"Calculated {hours:.2f} active hours from state logs for driver {driver_uuid}")

        return round(hours, 2)

    def calculate_hours_from_ride_durations(self, driver_uuid: str, start_date: datetime, end_date: datetime) -> float:
        """
        Calculate hours by summing ride durations plus reasonable gaps
        This gives a more realistic "active hours" estimate
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            # Build date filter
            if start_date and end_date:
                start_ts = int(start_date.timestamp())
                end_ts = int(end_date.timestamp())
                date_filter = "AND order_finished_timestamp >= ? AND order_finished_timestamp <= ?"
                params = (driver_uuid, start_ts, end_ts)
            else:
                date_filter = ""
                params = (driver_uuid,)

            # Get all orders with their timestamps
            cursor.execute(f'''
                SELECT 
                    order_accepted_timestamp,
                    order_finished_timestamp
                FROM orders
                WHERE driver_uuid = ?
                AND order_status = 'finished'
                AND order_accepted_timestamp IS NOT NULL
                AND order_finished_timestamp IS NOT NULL
                {date_filter}
                ORDER BY order_accepted_timestamp
            ''', params)

            orders = cursor.fetchall()

            if not orders:
                return 0.0

            # Method A: Sum ride durations + gaps between rides (up to 10 minutes per gap)
            total_seconds = 0
            last_finish_time = None

            for accepted, finished in orders:
                # Add ride duration
                ride_duration = finished - accepted
                total_seconds += ride_duration

                # Add gap time if this isn't the first ride
                if last_finish_time is not None:
                    gap = accepted - last_finish_time
                    # Count gaps up to 10 minutes as active time (waiting for next order)
                    if gap > 0 and gap <= 600:  # 600 seconds = 10 minutes
                        total_seconds += gap
                    elif gap > 600:
                        # For longer gaps, add max 10 minutes
                        total_seconds += 600

                last_finish_time = finished

            hours = total_seconds / 3600
            logger.info(f"Calculated {hours:.2f} active hours from {len(orders)} ride durations")

            return round(hours, 2)

    def calculate_active_hours(self, driver_uuid: str, start_date: datetime, end_date: datetime,
                               state_logs: Optional[List[Dict]] = None) -> float:
        """
        Smart calculation that uses state logs if available, otherwise ride durations
        """
        # Try Method 1: State logs (with Bolt's actual states)
        if state_logs:
            # Check if we have meaningful state data
            states_in_logs = set(
                log.get('state', '').lower() for log in state_logs if log.get('driver_uuid') == driver_uuid)

            # Only use state logs if they contain the expected states
            if any(state in states_in_logs for state in ['waiting_orders', 'has_order', 'inactive']):
                hours = self.calculate_hours_from_state_logs(driver_uuid, start_date, end_date, state_logs)
                if hours > 0:
                    return hours

        # Fallback to Method 2: Ride durations
        logger.info("Using ride duration calculation method")
        return self.calculate_hours_from_ride_durations(driver_uuid, start_date, end_date)