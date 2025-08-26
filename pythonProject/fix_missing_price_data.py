# Save as fix_missing_price_data.py

import asyncio
import os
import sqlite3
from datetime import datetime, timedelta
from dotenv import load_dotenv
from src.oauth.client import BoltOAuthClient
from src.api.bolt_client import BoltFleetClient

load_dotenv()


async def fix_orders_with_missing_price_data():
    """
    Re-fetch recent orders that have missing price data
    """

    oauth_client = BoltOAuthClient(
        client_id=os.getenv('BOLT_CLIENT_ID'),
        client_secret=os.getenv('BOLT_CLIENT_SECRET')
    )

    bolt_client = BoltFleetClient(oauth_client, company_id=172774)

    try:
        print("=== FIXING ORDERS WITH MISSING PRICE DATA ===\n")

        # Check database for orders with missing price data
        db_path = "fleet_data.db"
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()

            # Find orders with missing price data
            cursor.execute('''
                SELECT 
                    order_reference, driver_name, payment_method,
                    order_created_timestamp, order_finished_timestamp,
                    ride_price, net_earnings
                FROM orders 
                WHERE (ride_price IS NULL OR net_earnings IS NULL)
                AND order_status = 'finished'
                ORDER BY order_finished_timestamp DESC
                LIMIT 20
            ''')

            orders_with_missing_data = cursor.fetchall()

            if not orders_with_missing_data:
                print("✅ No orders found with missing price data!")
                return

            print(f"Found {len(orders_with_missing_data)} orders with missing price data:")
            for order in orders_with_missing_data:
                ref, name, payment, created, finished, ride_price, net = order
                finished_date = datetime.fromtimestamp(finished).strftime('%Y-%m-%d %H:%M')
                print(f"  - {ref[:20]}... ({name}, {payment}) on {finished_date}")
                print(f"    ride_price: {ride_price}, net_earnings: {net}")

        # Re-sync recent data to get updated price information
        print(f"\n🔄 Re-syncing recent orders to fix price data...")

        async with bolt_client:
            # Fetch last 7 days of orders to refresh any missing price data
            result = await bolt_client.sync_database(full_sync=False)
            print(f"Sync result: {result}")

        # Check if the missing data was fixed
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()

            cursor.execute('''
                SELECT COUNT(*) 
                FROM orders 
                WHERE (ride_price IS NULL OR net_earnings IS NULL)
                AND order_status = 'finished'
            ''')

            remaining_missing = cursor.fetchone()[0]

            if remaining_missing < len(orders_with_missing_data):
                print(f"✅ Fixed {len(orders_with_missing_data) - remaining_missing} orders!")
            else:
                print(f"⚠️ Still have {remaining_missing} orders with missing price data")
                print("   This might be normal for very recent orders that haven't been processed yet")

        # Show updated cash collection totals
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()

            cursor.execute('''
                SELECT 
                    SUM(CASE WHEN payment_method = 'cash' AND net_earnings IS NOT NULL THEN net_earnings ELSE 0 END) as cash_collected,
                    COUNT(CASE WHEN payment_method = 'cash' THEN 1 END) as cash_orders,
                    COUNT(CASE WHEN payment_method = 'cash' AND net_earnings IS NULL THEN 1 END) as cash_orders_missing_data
                FROM orders 
                WHERE order_status = 'finished'
            ''')

            cash_data = cursor.fetchone()
            print(f"\n💵 UPDATED CASH COLLECTION TOTALS:")
            print(f"   Cash collected: {cash_data[0]:.2f} RON")
            print(f"   Cash orders: {cash_data[1]}")
            print(f"   Cash orders missing data: {cash_data[2]}")

            if cash_data[2] > 0:
                print(f"   ⚠️ {cash_data[2]} cash orders still missing price data")

                # Show which cash orders are missing data
                cursor.execute('''
                    SELECT order_reference, order_finished_timestamp
                    FROM orders 
                    WHERE payment_method = 'cash' 
                    AND net_earnings IS NULL
                    AND order_status = 'finished'
                    ORDER BY order_finished_timestamp DESC
                ''')

                missing_cash_orders = cursor.fetchall()
                print(f"   Missing cash order data:")
                for ref, finished in missing_cash_orders:
                    finished_date = datetime.fromtimestamp(finished).strftime('%Y-%m-%d %H:%M')
                    print(f"     - {ref[:20]}... on {finished_date}")

        print(f"\n🎯 RECOMMENDATION:")
        print(f"   The website shows 3,788.9 RON cash collected")
        print(f"   Our current calculation: {cash_data[0]:.2f} RON")
        print(f"   Difference: {3788.9 - cash_data[0]:.2f} RON")

        if cash_data[2] > 0:
            print(f"   → {cash_data[2]} cash orders are missing price data - this explains part of the difference")
        else:
            print(f"   → All cash orders have price data, difference may be due to:")
            print(f"     • Different time periods")
            print(f"     • Website includes additional fees/tips")
            print(f"     • Different calculation method")

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await oauth_client.close()


if __name__ == "__main__":
    asyncio.run(fix_orders_with_missing_price_data())