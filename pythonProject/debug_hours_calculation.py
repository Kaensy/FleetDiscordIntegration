import asyncio
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
from src.oauth.client import BoltOAuthClient
from src.api.bolt_client import BoltFleetClient

load_dotenv()


async def debug_hours_calculation():
    """Debug the hours calculation to find discrepancies"""

    oauth_client = BoltOAuthClient(
        client_id=os.getenv('BOLT_CLIENT_ID'),
        client_secret=os.getenv('BOLT_CLIENT_SECRET')
    )

    fleet_client = BoltFleetClient(oauth_client, company_id=172774)

    # Test individual days
    test_cases = [
        {
            "name": "Aug 27 only (Yesterday)",
            "start": datetime(2025, 8, 27, 0, 0, 0),
            "end": datetime(2025, 8, 27, 23, 59, 59),
            "days_param": 2,  # This should give us yesterday when looking back 2 days
            "note": "Single day - Aug 27"
        },
        {
            "name": "Aug 28 only (Today)",
            "start": datetime(2025, 8, 28, 0, 0, 0),
            "end": datetime(2025, 8, 28, 23, 59, 59),
            "days_param": 1,  # This should give us today
            "note": "Single day - Aug 28"
        },
        {
            "name": "Both days (Aug 27-28)",
            "start": datetime(2025, 8, 27, 0, 0, 0),
            "end": datetime(2025, 8, 28, 23, 59, 59),
            "days_param": 2,
            "note": "Two full days"
        }
    ]

    driver_uuid = "d062347c-666e-49d1-ad99-63de3b9895a5"

    async with fleet_client:
        for test in test_cases:
            print(f"\n{'=' * 70}")
            print(f"Testing: {test['name']}")
            print(f"Period: {test['start'].strftime('%Y-%m-%d %H:%M')} to {test['end'].strftime('%Y-%m-%d %H:%M')}")
            print(f"Note: {test['note']}")
            print('=' * 70)

            # Get state logs for this specific period
            response = await fleet_client.get_fleet_state_logs(
                start_date=test['start'],
                end_date=test['end'],
                limit=1000
            )

            if response.get('code') != 0:
                print(f"Error fetching state logs: {response}")
                continue

            state_logs = response.get('data', {}).get('state_logs', [])
            driver_logs = [log for log in state_logs if log.get('driver_uuid') == driver_uuid]
            driver_logs.sort(key=lambda x: x.get('created', 0))

            print(f"Found {len(driver_logs)} state logs")

            # Manual calculation
            start_ts = int(test['start'].timestamp())
            end_ts = int(test['end'].timestamp())

            total_seconds = 0
            ride_seconds = 0
            waiting_seconds = 0

            last_state = None
            last_timestamp = None

            # Track online periods
            online_periods = []
            period_start = None

            for log in driver_logs:
                timestamp = log.get('created', 0)
                state = log.get('state', '').lower()
                dt = datetime.fromtimestamp(timestamp)

                # Track online/offline periods
                if state in ['waiting_orders', 'has_order', 'busy']:
                    if period_start is None:
                        period_start = dt
                elif state == 'inactive' and period_start:
                    online_periods.append((period_start, dt))
                    period_start = None

                # Calculate durations
                if last_timestamp and last_timestamp >= start_ts:
                    duration = min(timestamp, end_ts) - last_timestamp

                    if last_state == 'waiting_orders':
                        waiting_seconds += duration
                        total_seconds += duration
                    elif last_state in ['has_order', 'busy']:
                        ride_seconds += duration
                        total_seconds += duration

                last_state = state
                last_timestamp = max(timestamp, start_ts) if timestamp >= start_ts else last_timestamp

            # If still online at end
            if period_start:
                online_periods.append(
                    (period_start, datetime.fromtimestamp(min(end_ts, int(datetime.now().timestamp())))))

            # Calculate final period if needed
            if last_state in ['waiting_orders', 'has_order', 'busy'] and last_timestamp and last_timestamp < end_ts:
                duration = end_ts - last_timestamp
                if last_state == 'waiting_orders':
                    waiting_seconds += duration
                    total_seconds += duration
                elif last_state in ['has_order', 'busy']:
                    ride_seconds += duration
                    total_seconds += duration

            # Convert to hours
            total_hours = total_seconds / 3600
            ride_hours = ride_seconds / 3600
            waiting_hours = waiting_seconds / 3600

            print(f"\n📊 MANUAL CALCULATION:")
            print(f"  Total Online: {total_hours:.2f} hours")
            print(f"  ├─ On Rides: {ride_hours:.2f} hours")
            print(f"  └─ Waiting: {waiting_hours:.2f} hours")

            print(f"\n⏰ ONLINE PERIODS:")
            for start, end in online_periods:
                duration = (end - start).total_seconds() / 3600
                print(f"  {start.strftime('%H:%M')} - {end.strftime('%H:%M')} ({duration:.2f} hrs)")

            # Test database method with proper days parameter
            print(f"\n📊 DATABASE METHOD (using days={test['days_param']}):")
            stats = fleet_client.db.get_driver_stats_by_uuid(
                driver_uuid,
                days=test['days_param'],
                state_logs=state_logs
            )

            if stats:
                print(f"  Orders: {stats['orders_completed']}")
                print(f"  Total Online: {stats['hours_worked']} hours")
                print(f"  ├─ On Rides: {stats['ride_hours']} hours")
                print(f"  └─ Waiting: {stats['waiting_hours']} hours")
                print(f"  Date Range: {stats['date_range']}")
            else:
                print("  No stats returned!")

            # Get orders for comparison
            orders_response = await fleet_client.get_fleet_orders(
                start_date=test['start'],
                end_date=test['end'],
                limit=100
            )

            if orders_response.get('code') == 0:
                orders = orders_response.get('data', {}).get('orders', [])
                driver_orders = [o for o in orders if
                                 o.get('driver_uuid') == driver_uuid and o.get('order_status') == 'finished']
                print(f"\n📦 ORDERS: {len(driver_orders)} completed")

                # Calculate total ride time from orders
                total_ride_time_from_orders = 0
                for order in driver_orders:
                    if order.get('order_accepted_timestamp') and order.get('order_finished_timestamp'):
                        ride_duration = order['order_finished_timestamp'] - order['order_accepted_timestamp']
                        total_ride_time_from_orders += ride_duration

                print(f"  Ride time from orders: {total_ride_time_from_orders / 3600:.2f} hours")

    print(f"\n{'=' * 70}")
    print("SUMMARY:")
    print("The website's 'Active online time' likely refers to:")
    print("1. Total time online (rides + waiting) - if close to our Total Online")
    print("2. Only ride time - if close to our On Rides time")
    print("3. Something else - if neither matches")
    print(f"{'=' * 70}")

    await oauth_client.close()


if __name__ == "__main__":
    asyncio.run(debug_hours_calculation())