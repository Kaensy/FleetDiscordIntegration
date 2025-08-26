# Save this as test_cash_collection_fix.py

import asyncio
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
from src.oauth.client import BoltOAuthClient
from src.api.bolt_client import BoltFleetClient

load_dotenv()


async def analyze_cash_collection_methods():
    """
    Test different methods to calculate cash collection to match website data
    """

    oauth_client = BoltOAuthClient(
        client_id=os.getenv('BOLT_CLIENT_ID'),
        client_secret=os.getenv('BOLT_CLIENT_SECRET')
    )

    bolt_client = BoltFleetClient(oauth_client, company_id=172774)

    try:
        async with bolt_client:
            print("=== CASH COLLECTION ANALYSIS ===\n")

            # Get all orders for the driver
            start_date = datetime(2024, 7, 28)
            end_date = datetime(2024, 8, 26)

            orders_response = await bolt_client.get_fleet_orders(
                start_date=start_date,
                end_date=end_date,
                limit=1000
            )

            if orders_response.get('code') != 0:
                print(f"❌ Failed to fetch orders: {orders_response}")
                return

            all_orders = orders_response.get('data', {}).get('orders', [])

            # Find Muhammad Zaka's orders
            target_driver_orders = []
            for order in all_orders:
                if "Muhammad" in order.get('driver_name', '') or "Asad" in order.get('driver_name', ''):
                    if order.get('order_status') == 'finished':
                        target_driver_orders.append(order)

            print(f"Found {len(target_driver_orders)} finished orders for target driver")

            # Analyze cash orders with different calculation methods
            cash_orders = [o for o in target_driver_orders if o.get('payment_method') == 'cash']
            print(f"Cash orders: {len(cash_orders)}")

            if not cash_orders:
                print("❌ No cash orders found!")
                return

            # Method 1: Using net_earnings (current)
            method1_total = sum(order.get('order_price', {}).get('net_earnings', 0) or 0 for order in cash_orders)
            print(f"Method 1 (net_earnings): {method1_total:.2f} RON")

            # Method 2: Using ride_price - commission
            method2_total = 0
            for order in cash_orders:
                order_price = order.get('order_price', {}) or {}
                ride_price = order_price.get('ride_price', 0) or 0
                commission = order_price.get('commission', 0) or 0
                method2_total += (ride_price - commission)
            print(f"Method 2 (ride_price - commission): {method2_total:.2f} RON")

            # Method 3: Using ride_price only
            method3_total = sum(order.get('order_price', {}).get('ride_price', 0) or 0 for order in cash_orders)
            print(f"Method 3 (ride_price only): {method3_total:.2f} RON")

            # Method 4: Check if there's a different field
            print("\n=== SAMPLE CASH ORDER STRUCTURE ===")
            if cash_orders:
                sample_order = cash_orders[0]
                print("Order price structure:")
                order_price = sample_order.get('order_price', {})
                for key, value in order_price.items():
                    print(f"  {key}: {value}")

                # Check main order fields too
                print("\nMain order fields:")
                relevant_fields = ['payment_method', 'order_status', 'driver_name']
                for field in relevant_fields:
                    print(f"  {field}: {sample_order.get(field)}")

            print(f"\nTarget amount from website: 3,788.9 RON")
            print(f"Closest method: Method {1 if abs(3788.9 - method1_total) <= abs(3788.9 - method2_total) else 2}")

            # Method 5: Check if we need to include some non-cash orders
            print("\n=== CHECKING FOR MIXED PAYMENT SCENARIOS ===")

            all_payment_methods = set(order.get('payment_method') for order in target_driver_orders)
            print(f"All payment methods found: {all_payment_methods}")

            # Calculate for different payment method combinations
            for payment_method in all_payment_methods:
                if payment_method:
                    orders_of_type = [o for o in target_driver_orders if o.get('payment_method') == payment_method]
                    net_total = sum(
                        order.get('order_price', {}).get('net_earnings', 0) or 0 for order in orders_of_type)
                    print(f"{payment_method} orders: {len(orders_of_type)}, Net total: {net_total:.2f} RON")

            # Method 6: Maybe it's cash + something else?
            # Check if "Collected cash" includes tips or other components
            print("\n=== CHECKING FOR ADDITIONAL CASH COMPONENTS ===")

            # Look for any field that might represent tips or additional cash
            all_cash_earnings = 0
            for order in cash_orders:
                order_price = order.get('order_price', {}) or {}
                # Check all numeric fields that could be cash-related
                for key, value in order_price.items():
                    if isinstance(value, (int, float)) and value > 0:
                        if key.lower() in ['tip', 'cash', 'rider_tip', 'driver_tip']:
                            print(f"Found potential cash component '{key}': {value}")
                            all_cash_earnings += value

            if all_cash_earnings > 0:
                print(f"Total with additional cash components: {method1_total + all_cash_earnings:.2f} RON")

            # Final recommendation
            print(f"\n=== RECOMMENDATIONS ===")
            differences = [
                abs(3788.9 - method1_total),
                abs(3788.9 - method2_total),
                abs(3788.9 - method3_total)
            ]
            best_method = differences.index(min(differences)) + 1

            if min(differences) < 50:  # If within 50 RON
                print(f"✅ Method {best_method} is closest to website data")
                if best_method == 2:
                    print("   → Update database to use: ride_price - commission for cash orders")
                elif best_method == 3:
                    print("   → Update database to use: ride_price for cash orders")
            else:
                print("⚠️ None of the methods are very close to website data")
                print("   → Possible causes:")
                print("     - Different time period")
                print("     - Website includes additional fees/tips not in API")
                print("     - Website calculation includes pending orders")
                print("     - API response structure changed")

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await oauth_client.close()


if __name__ == "__main__":
    asyncio.run(analyze_cash_collection_methods())