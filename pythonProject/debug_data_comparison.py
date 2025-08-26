# Save this as debug_data_comparison.py

import asyncio
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
from src.oauth.client import BoltOAuthClient
from src.api.bolt_client import BoltFleetClient

load_dotenv()


async def debug_driver_data_comparison():
    """
    Debug script to compare our calculations with what should be correct
    """

    oauth_client = BoltOAuthClient(
        client_id=os.getenv('BOLT_CLIENT_ID'),
        client_secret=os.getenv('BOLT_CLIENT_SECRET')
    )

    # Initialize with company ID from config
    bolt_client = BoltFleetClient(oauth_client, company_id=172774)

    try:
        async with bolt_client:
            print("=== DEBUGGING DRIVER DATA DISCREPANCIES ===\n")

            # Get driver UUID - assuming Muhammad Zaka Ullah Asad based on images
            drivers = bolt_client.db.get_all_drivers()
            target_driver = None
            for num, uuid, name in drivers:
                if "Muhammad" in name or "Asad" in name:
                    target_driver = (uuid, name)
                    break

            if not target_driver:
                print("❌ Could not find target driver")
                return

            driver_uuid, driver_name = target_driver
            print(f"🎯 Analyzing data for: {driver_name}")
            print(f"   Driver UUID: {driver_uuid}")

            # Define time period (Jul 28 - Aug 26 based on image)
            start_date = datetime(2024, 7, 28)
            end_date = datetime(2024, 8, 26)
            print(f"📅 Time period: {start_date.strftime('%b %d')} - {end_date.strftime('%b %d')}")

            # 1. Fetch orders data
            print("\n1. FETCHING ORDERS DATA...")
            orders_response = await bolt_client.get_fleet_orders(
                start_date=start_date,
                end_date=end_date,
                limit=1000
            )

            if orders_response.get('code') != 0:
                print(f"❌ Failed to fetch orders: {orders_response}")
                return

            all_orders = orders_response.get('data', {}).get('orders', [])
            driver_orders = [o for o in all_orders if
                             o.get('driver_uuid') == driver_uuid and o.get('order_status') == 'finished']

            print(f"   Total orders in period: {len(all_orders)}")
            print(f"   Driver's finished orders: {len(driver_orders)}")

            # 2. Analyze cash collection
            print("\n2. ANALYZING CASH COLLECTION...")
            total_cash_collected = 0
            cash_orders = []

            for order in driver_orders:
                if order.get('payment_method') == 'cash':
                    cash_orders.append(order)
                    order_price = order.get('order_price', {}) or {}
                    net_earnings = order_price.get('net_earnings', 0) or 0
                    total_cash_collected += net_earnings

            print(f"   Cash orders found: {len(cash_orders)}")
            print(f"   Total cash collected (net_earnings): {total_cash_collected:.2f} RON")
            print(f"   Expected from website: 3,788.9 RON")
            print(f"   Difference: {3788.9 - total_cash_collected:.2f} RON")

            # Show first few cash orders for debugging
            print("\n   First 3 cash orders:")
            for i, order in enumerate(cash_orders[:3]):
                order_price = order.get('order_price', {}) or {}
                print(f"     {i + 1}. Order: {order.get('order_reference')}")
                print(f"        Ride price: {order_price.get('ride_price', 0):.2f}")
                print(f"        Net earnings: {order_price.get('net_earnings', 0):.2f}")
                print(f"        Commission: {order_price.get('commission', 0):.2f}")

            # 3. Fetch state logs for hours calculation
            print("\n3. FETCHING STATE LOGS...")
            state_response = await bolt_client.get_fleet_state_logs(
                start_date=start_date,
                end_date=end_date,
                limit=1000
            )

            state_logs = []
            if state_response.get('code') == 0:
                state_logs = state_response.get('data', {}).get('state_logs', [])
                driver_state_logs = [log for log in state_logs if log.get('driver_uuid') == driver_uuid]
                print(f"   Total state logs: {len(state_logs)}")
                print(f"   Driver's state logs: {len(driver_state_logs)}")

                # Show state log sample
                if driver_state_logs:
                    print(f"   First state log: {driver_state_logs[0]}")
                    print(f"   Last state log: {driver_state_logs[-1]}")
            else:
                print(f"   ❌ Failed to fetch state logs: {state_response}")

            # 4. Calculate hours using different methods
            print("\n4. CALCULATING HOURS WORKED...")

            # Method 1: Using state logs
            if state_logs:
                hours_from_states = bolt_client.db.calculate_online_hours_from_states(
                    driver_uuid, start_date, end_date, state_logs
                )
                print(f"   Hours from state logs: {hours_from_states:.1f}")

            # Method 2: Using order timestamps (current method)
            order_times = []
            for order in driver_orders:
                if order.get('order_accepted_timestamp') and order.get('order_finished_timestamp'):
                    order_times.append((
                        order.get('order_accepted_timestamp'),
                        order.get('order_finished_timestamp')
                    ))

            hours_from_orders = bolt_client.db._calculate_realistic_hours(order_times)
            print(f"   Hours from order times (realistic): {hours_from_orders:.1f}")

            # Method 3: Simple first-to-last calculation (old method)
            if order_times:
                order_times.sort()
                simple_hours = (order_times[-1][1] - order_times[0][0]) / 3600
                print(f"   Hours from first-to-last order: {simple_hours:.1f}")

            print(f"   Expected from website: 83.6 hours")

            # 5. Get current database stats
            print("\n5. CURRENT DATABASE STATS...")
            current_stats = bolt_client.db.get_driver_stats_by_uuid(
                driver_uuid,
                days=None,  # All time
                state_logs=state_logs
            )

            if current_stats:
                print(f"   Database shows:")
                print(f"     Orders: {current_stats['orders_completed']}")
                print(f"     Gross earnings: {current_stats['gross_earnings']} RON")
                print(f"     Net earnings: {current_stats['net_earnings']} RON")
                print(f"     Cash collected: {current_stats['cash_collected']} RON")
                print(f"     Hours worked: {current_stats['hours_worked']} hrs")
                print(f"     Distance: {current_stats['total_distance']} km")

            # 6. Show earnings breakdown
            print("\n6. EARNINGS BREAKDOWN...")
            total_gross = sum(order.get('order_price', {}).get('ride_price', 0) or 0 for order in driver_orders)
            total_net = sum(order.get('order_price', {}).get('net_earnings', 0) or 0 for order in driver_orders)
            total_commission = sum(order.get('order_price', {}).get('commission', 0) or 0 for order in driver_orders)

            print(f"   Our calculation:")
            print(f"     Gross earnings: {total_gross:.2f} RON")
            print(f"     Net earnings: {total_net:.2f} RON")
            print(f"     Commission: {total_commission:.2f} RON")
            print(f"   Website shows:")
            print(f"     Gross earnings: 9,109.3 RON")
            print(f"     Net earnings: 7,014.13 RON")
            print(f"     Missing: {9109.3 - total_gross:.2f} RON (likely Fleet Campaign + Reimbursements)")

    except Exception as e:
        print(f"❌ Error during analysis: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await oauth_client.close()


if __name__ == "__main__":
    asyncio.run(debug_driver_data_comparison())