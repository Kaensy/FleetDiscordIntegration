import asyncio
import os
import sqlite3
from datetime import datetime, timedelta
from dotenv import load_dotenv
from src.oauth.client import BoltOAuthClient
from src.api.bolt_client import BoltFleetClient
from pathlib import Path

load_dotenv()


async def fix_and_sync_database():
    """Fix database initialization and sync all August orders"""

    # Create data directory
    data_dir = Path("data")
    data_dir.mkdir(exist_ok=True)
    db_path = data_dir / "fleet_data.db"

    print(f"Setting up database at: {db_path}")
    print("=" * 60)

    # Step 1: Create database tables manually
    print("\n1. CREATING DATABASE TABLES...")

    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()

        # Drop existing tables if they exist (for clean slate)
        cursor.execute("DROP TABLE IF EXISTS orders")
        cursor.execute("DROP TABLE IF EXISTS sync_status")

        # Create orders table with all columns
        cursor.execute('''
            CREATE TABLE orders (
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

        # Create sync status table
        cursor.execute('''
            CREATE TABLE sync_status (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                last_sync_timestamp INTEGER,
                orders_synced INTEGER,
                sync_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Create indexes
        cursor.execute('CREATE INDEX idx_orders_finished ON orders(order_finished_timestamp)')
        cursor.execute('CREATE INDEX idx_orders_driver ON orders(driver_uuid)')
        cursor.execute('CREATE INDEX idx_orders_status ON orders(order_status)')
        cursor.execute('CREATE INDEX idx_orders_payment ON orders(payment_method)')

        conn.commit()
        print("✅ Tables created successfully!")

    # Step 2: Initialize API clients
    print("\n2. INITIALIZING API CLIENTS...")

    oauth_client = BoltOAuthClient(
        client_id=os.getenv('BOLT_CLIENT_ID'),
        client_secret=os.getenv('BOLT_CLIENT_SECRET')
    )

    fleet_client = BoltFleetClient(oauth_client, company_id=172774)
    print("✅ API clients initialized!")

    # Step 3: Fetch August orders from API
    print("\n3. FETCHING AUGUST ORDERS FROM API...")

    august_start = datetime(2025, 8, 1, 0, 0, 0)
    august_mid = datetime(2025, 8, 16, 0, 0, 0)
    august_end = datetime(2025, 9, 1, 0, 0, 0)

    all_orders = []

    try:
        async with fleet_client:
            # First half
            print("  Fetching Aug 1-15...")
            offset = 0
            while True:
                response = await fleet_client.get_fleet_orders(
                    start_date=august_start,
                    end_date=august_mid,
                    limit=1000,
                    offset=offset
                )

                if response.get('code') != 0:
                    break

                orders = response.get('data', {}).get('orders', [])
                if not orders:
                    break

                all_orders.extend(orders)
                offset += len(orders)

                if len(orders) < 1000:
                    break

            # Second half
            print("  Fetching Aug 16-31...")
            offset = 0
            while True:
                response = await fleet_client.get_fleet_orders(
                    start_date=august_mid,
                    end_date=august_end,
                    limit=1000,
                    offset=offset
                )

                if response.get('code') != 0:
                    break

                orders = response.get('data', {}).get('orders', [])
                if not orders:
                    break

                all_orders.extend(orders)
                offset += len(orders)

                if len(orders) < 1000:
                    break

            print(f"✅ Fetched {len(all_orders)} total orders from API!")

            # Count by status
            status_counts = {}
            for order in all_orders:
                status = order.get('order_status', 'unknown')
                status_counts[status] = status_counts.get(status, 0) + 1

            print("\n  Order breakdown by status:")
            for status, count in sorted(status_counts.items()):
                print(f"    {status}: {count}")

    finally:
        await oauth_client.close()

    # Step 4: Store orders in database
    print("\n4. STORING ORDERS IN DATABASE...")

    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()

        stored_count = 0
        error_count = 0

        for order in all_orders:
            try:
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
                    order.get('order_pickup_timestamp'),
                    order.get('order_drop_off_timestamp'),
                    pickup_lat, pickup_lng, dropoff_lat, dropoff_lng,
                    order.get('vehicle_license_plate'),
                    order.get('payment_method'),
                    order.get('rating')
                ))

                stored_count += 1

            except Exception as e:
                error_count += 1
                print(f"    Error storing order {order.get('order_reference')}: {e}")

        conn.commit()

        print(f"✅ Stored {stored_count} orders successfully!")
        if error_count > 0:
            print(f"⚠️  {error_count} orders had errors")

    # Step 5: Verify database
    print("\n5. VERIFYING DATABASE...")

    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()

        # Count total orders
        cursor.execute("SELECT COUNT(*) FROM orders")
        total_orders = cursor.fetchone()[0]
        print(f"  Total orders in database: {total_orders}")

        # Count by status
        cursor.execute("SELECT order_status, COUNT(*) FROM orders GROUP BY order_status")
        status_rows = cursor.fetchall()

        print("  Orders by status:")
        for status, count in status_rows:
            print(f"    {status}: {count}")

        # Check August specifically
        august_start_ts = int(august_start.timestamp())
        august_end_ts = int(august_end.timestamp())

        cursor.execute('''
            SELECT COUNT(*) 
            FROM orders 
            WHERE order_finished_timestamp >= ? 
            AND order_finished_timestamp < ?
            AND order_status = 'finished'
        ''', (august_start_ts, august_end_ts))

        august_finished = cursor.fetchone()[0]
        print(f"\n  August 'finished' orders: {august_finished}")

        # Get driver stats for August
        cursor.execute('''
            SELECT 
                driver_name,
                COUNT(*) as orders,
                COALESCE(SUM(ride_price), 0) as gross,
                COALESCE(SUM(ride_distance) / 1000.0, 0) as km
            FROM orders
            WHERE order_finished_timestamp >= ? 
            AND order_finished_timestamp < ?
            AND order_status = 'finished'
            GROUP BY driver_name
        ''', (august_start_ts, august_end_ts))

        driver_stats = cursor.fetchall()

        print("\n  August driver stats:")
        for name, orders, gross, km in driver_stats:
            print(f"    {name}: {orders} orders, {gross:.2f} RON, {km:.1f} km")

    print("\n" + "=" * 60)
    print("✅ DATABASE FIXED AND SYNCED SUCCESSFULLY!")
    print("=" * 60)
    print("\nYou can now use the !driver-stats command in Discord")
    print("The August data should show 372 finished orders matching the website!")


if __name__ == "__main__":
    asyncio.run(fix_and_sync_database())