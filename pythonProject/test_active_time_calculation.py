# Save as test_active_time_calculation_fixed.py

import asyncio
import os
import sqlite3
from datetime import datetime, timedelta
from dotenv import load_dotenv
from src.oauth.client import BoltOAuthClient
from src.api.bolt_client import BoltFleetClient

load_dotenv()


async def test_active_time_calculation():
    """
    Test the ACTIVE time calculation to match website's "Active online time"
    """

    oauth_client = BoltOAuthClient(
        client_id=os.getenv('BOLT_CLIENT_ID'),
        client_secret=os.getenv('BOLT_CLIENT_SECRET')
    )

    bolt_client = BoltFleetClient(oauth_client, company_id=172774)

    try:
        print("=== TESTING ACTIVE TIME CALCULATION ===\n")

        # Get Muhammad's data from database directly
        db_path = "fleet_data.db"
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()

            # Check what columns exist
            cursor.execute("PRAGMA table_info(orders)")
            columns = [column[1] for column in cursor.fetchall()]
            print(f"📋 Available columns: {columns}")

            # Find Muhammad
            cursor.execute('''
                SELECT driver_uuid, driver_name 
                FROM orders 
                WHERE driver_name LIKE '%Muhammad%' 
                LIMIT 1
            ''')

            driver_data = cursor.fetchone()
            if not driver_data:
                print("❌ Could not find Muhammad in database")
                return

            driver_uuid, driver_name = driver_data
            print(f"🎯 Testing for: {driver_name}")

            # Get orders for "today" - test for recent date with data
            # Let's test for Aug 26 to match current data
            test_date = datetime.now() - timedelta(days=1)  # Yesterday since we just passed midnight
            start_of_day = datetime(test_date.year, test_date.month, test_date.day, 0, 0, 0)
            end_of_day = datetime(test_date.year, test_date.month, test_date.day, 23, 59, 59)
            start_ts = int(start_of_day.timestamp())
            end_ts = int(end_of_day.timestamp())

            print(f"📅 Testing date: {test_date.strftime('%Y-%m-%d')}")
            print(f"   Time range: {start_of_day} to {end_of_day}")

            # Build query with available columns only - SAFE VERSION
            base_columns = '''
                order_accepted_timestamp,
                order_finished_timestamp
            '''

            optional_columns = []
            if 'order_pickup_timestamp' in columns:
                optional_columns.append('order_pickup_timestamp')
            if 'order_drop_off_timestamp' in columns:
                optional_columns.append('order_drop_off_timestamp')

            if optional_columns:
                query_columns = base_columns + ', ' + ', '.join(optional_columns)
            else:
                query_columns = base_columns

            print(f"📊 Query columns: {query_columns}")

            # Get orders for this day
            cursor.execute(f'''
                SELECT 
                    {query_columns},
                    order_reference
                FROM orders
                WHERE driver_uuid = ? 
                AND order_status = 'finished'
                AND order_finished_timestamp >= ? 
                AND order_finished_timestamp <= ?
                AND order_accepted_timestamp IS NOT NULL
                AND order_finished_timestamp IS NOT NULL
                ORDER BY order_accepted_timestamp
            ''', (driver_uuid, start_ts, end_ts))

            orders_for_day = cursor.fetchall()

            print(f"📋 Orders found for {test_date.strftime('%Y-%m-%d')}: {len(orders_for_day)}")

            if not orders_for_day:
                print("❌ No orders found for test date")
                print("   Trying to find any recent orders...")

                # Find any orders for this driver
                cursor.execute('''
                    SELECT COUNT(*) as total_orders,
                           MIN(order_finished_timestamp) as first_order,
                           MAX(order_finished_timestamp) as last_order
                    FROM orders
                    WHERE driver_uuid = ? 
                    AND order_status = 'finished'
                ''', (driver_uuid,))

                stats = cursor.fetchone()
                if stats and stats[0] > 0:
                    first_date = datetime.fromtimestamp(stats[1]).strftime('%Y-%m-%d')
                    last_date = datetime.fromtimestamp(stats[2]).strftime('%Y-%m-%d')
                    print(f"   Found {stats[0]} total orders from {first_date} to {last_date}")

                    # Test with the most recent day that has data
                    recent_date = datetime.fromtimestamp(stats[2])
                    start_recent = datetime(recent_date.year, recent_date.month, recent_date.day, 0, 0, 0)
                    end_recent = datetime(recent_date.year, recent_date.month, recent_date.day, 23, 59, 59)
                    start_recent_ts = int(start_recent.timestamp())
                    end_recent_ts = int(end_recent.timestamp())

                    cursor.execute(f'''
                        SELECT 
                            {query_columns},
                            order_reference
                        FROM orders
                        WHERE driver_uuid = ? 
                        AND order_status = 'finished'
                        AND order_finished_timestamp >= ? 
                        AND order_finished_timestamp <= ?
                        AND order_accepted_timestamp IS NOT NULL
                        AND order_finished_timestamp IS NOT NULL
                        ORDER BY order_accepted_timestamp
                    ''', (driver_uuid, start_recent_ts, end_recent_ts))

                    orders_for_day = cursor.fetchall()
                    test_date = recent_date
                    print(f"   Using {test_date.strftime('%Y-%m-%d')} with {len(orders_for_day)} orders")

                if not orders_for_day:
                    print("❌ Still no orders found")
                    return

            # Calculate ACTIVE time manually to verify
            total_active_minutes = 0

            print(f"\n🔍 ORDER-BY-ORDER ACTIVE TIME BREAKDOWN:")
            for i, order_data in enumerate(orders_for_day, 1):
                accepted = order_data[0]
                finished = order_data[1]
                # order_reference is the last column
                ref = order_data[-1]

                if not accepted or not finished:
                    continue

                # ACTIVE time = accepted to finished (total engagement time)
                active_seconds = finished - accepted
                active_minutes = active_seconds / 60

                # Format times
                accepted_time = datetime.fromtimestamp(accepted).strftime('%H:%M')
                finished_time = datetime.fromtimestamp(finished).strftime('%H:%M')

                print(f"   Order {i}: {accepted_time} → {finished_time}")
                print(f"     Reference: {ref[:20]}...")
                print(f"     Active time: {active_minutes:.1f} minutes")

                # Cap at 120 minutes per order (sanity check)
                active_minutes = min(active_minutes, 120)
                total_active_minutes += active_minutes

            total_active_hours = total_active_minutes / 60

            print(f"\n📊 MANUAL CALCULATION RESULTS:")
            print(f"   Total active minutes: {total_active_minutes:.1f}")
            print(f"   Total active hours: {total_active_hours:.2f}")
            print(f"   Website target: 2h 38min = {2 + (38 / 60):.2f} hours")

            if total_active_hours > 0:
                difference_minutes = abs(total_active_minutes - (2 * 60 + 38))
                print(f"   Difference: {difference_minutes:.1f} minutes")

                if difference_minutes < 15:
                    print("   ✅ EXCELLENT MATCH! (within 15 minutes)")
                elif difference_minutes < 30:
                    print("   ✅ GOOD MATCH! (within 30 minutes)")
                elif difference_minutes < 60:
                    print("   ⚠️ Acceptable difference (within 1 hour)")
                else:
                    print("   ❌ Large difference")

            # Now test with the actual database method
            print(f"\n🧮 TESTING DATABASE METHOD:")

            # Test the improved calculation for 1 day
            days = 1
            stats = bolt_client.db.get_driver_stats_by_uuid(driver_uuid, days, [])

            if stats:
                print(f"   Database calculation: {stats['hours_worked']} hours")
                print(f"   Orders: {stats['orders_completed']}")
                print(f"   Date range: {stats['date_range']}")

                if stats['hours_worked'] > 0:
                    website_target = 2 + 38 / 60
                    db_difference = abs(stats['hours_worked'] - website_target)
                    print(f"   Difference from website target: {db_difference:.2f} hours")

                    if db_difference < 0.5:
                        print("   ✅ DATABASE METHOD WORKS!")
                    elif db_difference < 1.0:
                        print("   ⚠️ Database method is close")
                    else:
                        print("   ❌ Database method needs adjustment")
                else:
                    print("   ⚠️ No hours calculated")
            else:
                print("   ❌ No stats returned from database method")

        print(f"\n🎯 SUMMARY:")
        print(f"   • Website shows 'Active online time: 2h 38min'")
        print(f"   • This is time actively engaged with customers")
        print(f"   • Our calculation: sum of (order accepted → order finished)")
        print(f"   • Expected match: within 15-30 minutes is excellent")
        print(f"   • Test completed for date: {test_date.strftime('%Y-%m-%d')}")

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await oauth_client.close()


if __name__ == "__main__":
    asyncio.run(test_active_time_calculation())