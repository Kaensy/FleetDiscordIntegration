import asyncio
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
from src.oauth.client import BoltOAuthClient
from src.api.bolt_client import BoltFleetClient
from src.utils.database import FleetDatabase
import sqlite3

load_dotenv()


async def debug_august_orders():
    """Debug August 2025 order discrepancies"""

    # Initialize clients
    oauth_client = BoltOAuthClient(
        client_id=os.getenv('BOLT_CLIENT_ID'),
        client_secret=os.getenv('BOLT_CLIENT_SECRET')
    )

    fleet_client = BoltFleetClient(oauth_client, company_id=172774)

    # Initialize database (this creates tables if they don't exist)
    db = FleetDatabase()
    print(f"Database initialized at: {db.db_path}")

    # Force database initialization
    db.init_database()

    # Define August 2025 date range - split into two parts to avoid API limit
    august_start = datetime(2025, 8, 1, 0, 0, 0)
    august_mid = datetime(2025, 8, 16, 0, 0, 0)  # Split month in half
    august_end = datetime(2025, 9, 1, 0, 0, 0)

    print(f"Debugging orders for August 2025")
    print(f"Date range: {august_start} to {august_end}")
    print("=" * 60)

    try:
        async with fleet_client:
            # 1. Fetch ALL orders from API for August (in two chunks to avoid 31-day limit)
            print("\n1. FETCHING FROM API...")
            all_api_orders = []

            # Fetch first half of month
            print("  Fetching first half of August (1-15)...")
            offset = 0
            while True:
                response = await fleet_client.get_fleet_orders(
                    start_date=august_start,
                    end_date=august_mid,
                    limit=1000,
                    offset=offset
                )

                if response.get('code') != 0:
                    print(f"API Error: {response}")
                    break

                orders = response.get('data', {}).get('orders', [])
                if not orders:
                    break

                all_api_orders.extend(orders)
                offset += len(orders)
                print(f"    Fetched {len(orders)} orders (total: {len(all_api_orders)})")

                if len(orders) < 1000:
                    break

            # Fetch second half of month
            print("  Fetching second half of August (16-31)...")
            offset = 0
            while True:
                response = await fleet_client.get_fleet_orders(
                    start_date=august_mid,
                    end_date=august_end,
                    limit=1000,
                    offset=offset
                )

                if response.get('code') != 0:
                    print(f"API Error: {response}")
                    break

                orders = response.get('data', {}).get('orders', [])
                if not orders:
                    break

                all_api_orders.extend(orders)
                offset += len(orders)
                print(f"    Fetched {len(orders)} orders (total: {len(all_api_orders)})")

                if len(orders) < 1000:
                    break

            print(f"\nTotal API orders for August: {len(all_api_orders)}")

            # 2. Analyze order statuses
            print("\n2. ORDER STATUS BREAKDOWN:")
            status_counts = {}
            driver_orders = {}

            for order in all_api_orders:
                status = order.get('order_status', 'unknown')
                driver_name = order.get('driver_name', 'Unknown')

                status_counts[status] = status_counts.get(status, 0) + 1

                if driver_name not in driver_orders:
                    driver_orders[driver_name] = {
                        'total': 0,
                        'statuses': {},
                        'distance': 0,
                        'gross': 0,
                        'net': 0,
                        'cash_gross': 0,
                        'cash_net': 0,
                        'orders': []
                    }

                driver_orders[driver_name]['total'] += 1
                driver_orders[driver_name]['statuses'][status] = driver_orders[driver_name]['statuses'].get(status,
                                                                                                            0) + 1

                # Add financial data
                if order.get('order_price'):
                    driver_orders[driver_name]['distance'] += (order.get('ride_distance', 0) or 0)
                    gross = order['order_price'].get('ride_price', 0) or 0
                    net = order['order_price'].get('net_earnings', 0) or 0
                    driver_orders[driver_name]['gross'] += gross
                    driver_orders[driver_name]['net'] += net

                    # Track cash payments
                    if order.get('payment_method') == 'cash':
                        driver_orders[driver_name]['cash_gross'] += gross
                        driver_orders[driver_name]['cash_net'] += net

                # Store first 5 orders for debugging
                if len(driver_orders[driver_name]['orders']) < 5:
                    driver_orders[driver_name]['orders'].append({
                        'ref': order.get('order_reference'),
                        'status': status,
                        'created': datetime.fromtimestamp(order.get('order_created_timestamp', 0)).strftime(
                            '%Y-%m-%d %H:%M') if order.get('order_created_timestamp') else 'N/A',
                        'finished': datetime.fromtimestamp(order.get('order_finished_timestamp', 0)).strftime(
                            '%Y-%m-%d %H:%M') if order.get('order_finished_timestamp') else 'N/A'
                    })

            for status, count in sorted(status_counts.items()):
                print(f"  {status}: {count} orders")

            # 3. Show driver breakdown
            print("\n3. DRIVER BREAKDOWN:")
            for driver_name, data in driver_orders.items():
                print(f"\n  {driver_name}:")
                print(f"    Total orders: {data['total']}")
                print(f"    Distance: {data['distance'] / 1000:.1f} km")
                print(f"    Gross: {data['gross']:.2f} RON")
                print(f"    Net: {data['net']:.2f} RON")
                print(f"    Cash Gross: {data['cash_gross']:.2f} RON")
                print(f"    Cash Net: {data['cash_net']:.2f} RON")
                print(f"    Statuses: {data['statuses']}")
                print(f"    Sample orders:")
                for order in data['orders'][:3]:
                    print(
                        f"      - {order['ref']}: {order['status']} (created: {order['created']}, finished: {order['finished']})")

            # 4. Check database
            print("\n4. DATABASE COMPARISON:")
            db_path = "fleet_data.db"

            with sqlite3.connect(db_path) as conn:
                cursor = conn.cursor()

                # Get August orders from database
                august_start_ts = int(august_start.timestamp())
                august_end_ts = int(august_end.timestamp())

                # Count ALL orders in database for August (any status)
                cursor.execute('''
                    SELECT 
                        order_status,
                        COUNT(*) as count
                    FROM orders
                    WHERE order_finished_timestamp >= ? 
                    AND order_finished_timestamp < ?
                    GROUP BY order_status
                ''', (august_start_ts, august_end_ts))

                db_status_counts = cursor.fetchall()

                print("  Database order statuses:")
                total_db = 0
                for status, count in db_status_counts:
                    print(f"    {status}: {count}")
                    total_db += count
                print(f"  Total in DB: {total_db}")

                # Check for specific driver
                cursor.execute('''
                    SELECT 
                        driver_name,
                        COUNT(*) as total,
                        COUNT(CASE WHEN order_status = 'finished' THEN 1 END) as finished,
                        COUNT(CASE WHEN order_status != 'finished' THEN 1 END) as other,
                        GROUP_CONCAT(DISTINCT order_status) as all_statuses,
                        COALESCE(SUM(ride_distance) / 1000.0, 0) as total_km,
                        COALESCE(SUM(ride_price), 0) as gross,
                        COALESCE(SUM(net_earnings), 0) as net,
                        COALESCE(SUM(CASE WHEN payment_method = 'cash' THEN ride_price ELSE 0 END), 0) as cash_gross
                    FROM orders
                    WHERE order_finished_timestamp >= ? 
                    AND order_finished_timestamp < ?
                    GROUP BY driver_name
                ''', (august_start_ts, august_end_ts))

                print("\n  Database driver stats:")
                for row in cursor.fetchall():
                    print(f"    {row[0]}:")
                    print(f"      Total: {row[1]} (finished: {row[2]}, other: {row[3]})")
                    print(f"      All statuses in DB: {row[4]}")
                    print(f"      Distance: {row[5]:.1f} km")
                    print(f"      Gross: {row[6]:.2f} RON")
                    print(f"      Net: {row[7]:.2f} RON")
                    print(f"      Cash collected: {row[8]:.2f} RON")

                # 5. Find missing orders
                print("\n5. CHECKING FOR MISSING ORDERS:")

                # Get all order references from API
                api_refs = {order.get('order_reference') for order in all_api_orders if order.get('order_reference')}

                # Get all order references from database
                cursor.execute('''
                    SELECT order_reference 
                    FROM orders
                    WHERE order_finished_timestamp >= ? 
                    AND order_finished_timestamp < ?
                ''', (august_start_ts, august_end_ts))

                db_refs = {row[0] for row in cursor.fetchall()}

                missing_in_db = api_refs - db_refs
                extra_in_db = db_refs - api_refs

                print(f"  Orders in API: {len(api_refs)}")
                print(f"  Orders in DB: {len(db_refs)}")
                print(f"  Missing in DB: {len(missing_in_db)}")
                print(f"  Extra in DB: {len(extra_in_db)}")

                if missing_in_db:
                    print("\n  Sample missing orders:")
                    for ref in list(missing_in_db)[:10]:
                        order = next(o for o in all_api_orders if o.get('order_reference') == ref)
                        print(f"    {ref}:")
                        print(f"      Status: {order.get('order_status')}")
                        print(f"      Driver: {order.get('driver_name')}")
                        print(
                            f"      Created: {datetime.fromtimestamp(order.get('order_created_timestamp', 0)).strftime('%Y-%m-%d %H:%M')}")
                        print(
                            f"      Finished: {datetime.fromtimestamp(order.get('order_finished_timestamp', 0)).strftime('%Y-%m-%d %H:%M') if order.get('order_finished_timestamp') else 'N/A'}")

                # 6. Sync missing orders
                if missing_in_db:
                    user_input = input(f"\n  Do you want to sync {len(missing_in_db)} missing orders? (yes/no): ")

                    if user_input.lower() == 'yes':
                        print(f"\n6. SYNCING {len(missing_in_db)} MISSING ORDERS...")

                        synced = 0
                        for order in all_api_orders:
                            if order.get('order_reference') in missing_in_db:
                                if db._store_order(conn, order):
                                    synced += 1

                        conn.commit()
                        print(f"  Synced {synced} orders successfully!")

                        # Re-check stats
                        cursor.execute('''
                            SELECT 
                                COUNT(*) as total,
                                COUNT(CASE WHEN order_status = 'finished' THEN 1 END) as finished
                            FROM orders
                            WHERE order_finished_timestamp >= ? 
                            AND order_finished_timestamp < ?
                        ''', (august_start_ts, august_end_ts))

                        row = cursor.fetchone()
                        print(f"  New total orders in DB: {row[0]} (finished: {row[1]})")

    finally:
        await oauth_client.close()


if __name__ == "__main__":
    asyncio.run(debug_august_orders())