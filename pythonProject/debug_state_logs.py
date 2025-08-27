# debug_state_logs.py - Debug what state logs we're getting from the API
import asyncio
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
from src.oauth.client import BoltOAuthClient
from src.api.bolt_client import BoltFleetClient
import json

load_dotenv()


async def debug_state_logs():
    """Debug state logs to understand what data we're getting"""

    oauth_client = BoltOAuthClient(
        client_id=os.getenv('BOLT_CLIENT_ID'),
        client_secret=os.getenv('BOLT_CLIENT_SECRET')
    )

    fleet_client = BoltFleetClient(oauth_client, company_id=os.getenv('BOLT_COMPANY_ID'))

    try:
        async with fleet_client:
            # Get state logs for the last 2 days
            start_date = datetime.now() - timedelta(days=2)
            end_date = datetime.now()

            print("=" * 60)
            print(f"FETCHING STATE LOGS FROM {start_date} TO {end_date}")
            print("=" * 60)

            response = await fleet_client.get_fleet_state_logs(
                start_date=start_date,
                end_date=end_date,
                limit=1000
            )

            print(f"\nAPI Response Code: {response.get('code')}")
            print(f"API Message: {response.get('message', 'OK')}")

            if response.get('code') == 0:
                state_logs = response.get('data', {}).get('state_logs', [])
                print(f"\nTotal state logs received: {len(state_logs)}")

                if state_logs:
                    # Group by driver
                    drivers = {}
                    for log in state_logs:
                        driver_uuid = log.get('driver_uuid')
                        if driver_uuid not in drivers:
                            drivers[driver_uuid] = []
                        drivers[driver_uuid].append(log)

                    print(f"Unique drivers in logs: {len(drivers)}")

                    # Analyze first driver's logs
                    for driver_uuid, logs in list(drivers.items())[:1]:  # Just first driver
                        print(f"\n{'=' * 60}")
                        print(f"DRIVER: {driver_uuid[:8]}...")
                        print(f"Total logs: {len(logs)}")
                        print("=" * 60)

                        # Sort by timestamp
                        logs.sort(key=lambda x: x.get('created', 0))

                        # Show structure of first log
                        if logs:
                            print("\nSample log structure:")
                            print(json.dumps(logs[0], indent=2))

                        # Analyze state transitions
                        print("\nState transitions (last 10):")
                        for log in logs[-10:]:
                            timestamp = log.get('created', 0)
                            dt = datetime.fromtimestamp(timestamp) if timestamp else None
                            state = log.get('state', 'unknown')
                            vehicle = log.get('vehicle_uuid', 'no-vehicle')[:8]

                            print(f"  {dt}: {state} (vehicle: {vehicle}...)")

                        # Calculate online hours using different methods
                        print("\n" + "=" * 60)
                        print("CALCULATING ACTIVE HOURS")
                        print("=" * 60)

                        # Method 1: State-based calculation
                        online_seconds = 0
                        online_start = None

                        for log in logs:
                            state = log.get('state', '').lower()
                            timestamp = log.get('created', 0)

                            if state in ['active', 'online', 'busy']:
                                if online_start is None:
                                    online_start = timestamp
                                    print(f"  ONLINE at {datetime.fromtimestamp(timestamp)}")
                            elif state in ['inactive', 'offline'] and online_start:
                                duration = timestamp - online_start
                                online_seconds += duration
                                print(
                                    f"  OFFLINE at {datetime.fromtimestamp(timestamp)} (duration: {duration / 3600:.2f} hrs)")
                                online_start = None

                        # If still online at end
                        if online_start:
                            duration = int(end_date.timestamp()) - online_start
                            online_seconds += duration
                            print(f"  Still online at end (duration: {duration / 3600:.2f} hrs)")

                        print(f"\nMETHOD 1 - State-based: {online_seconds / 3600:.2f} hours")

                        # Show unique states found
                        unique_states = set(log.get('state', 'unknown') for log in logs)
                        print(f"\nUnique states found: {unique_states}")

                else:
                    print("\n❌ No state logs returned!")
                    print("This might mean:")
                    print("  1. The getFleetStateLogs endpoint doesn't return online/offline data")
                    print("  2. We need to use a different endpoint")
                    print("  3. We need to calculate from order durations instead")

            # Now let's check order data for ride durations
            print("\n" + "=" * 60)
            print("CHECKING ORDER DATA FOR RIDE DURATIONS")
            print("=" * 60)

            orders_response = await fleet_client.get_fleet_orders(
                start_date=start_date,
                end_date=end_date,
                limit=100
            )

            if orders_response.get('code') == 0:
                orders = orders_response.get('data', {}).get('orders', [])
                print(f"Orders fetched: {len(orders)}")

                if orders:
                    # Check first order structure
                    print("\nFirst order structure (looking for duration fields):")
                    first_order = orders[0]

                    # Check for duration-related fields
                    duration_fields = [
                        'ride_duration', 'duration', 'trip_duration',
                        'order_duration', 'ride_duration_seconds'
                    ]

                    for field in duration_fields:
                        if field in first_order:
                            print(f"  ✅ Found '{field}': {first_order[field]}")

                    # Check timestamp fields
                    print("\nTimestamp fields:")
                    timestamp_fields = [
                        'order_created_timestamp', 'order_accepted_timestamp',
                        'order_finished_timestamp', 'pickup_timestamp', 'dropoff_timestamp'
                    ]

                    for field in timestamp_fields:
                        if field in first_order and first_order[field]:
                            dt = datetime.fromtimestamp(first_order[field])
                            print(f"  {field}: {dt}")

                    # Calculate duration from timestamps
                    if first_order.get('order_accepted_timestamp') and first_order.get('order_finished_timestamp'):
                        duration = first_order['order_finished_timestamp'] - first_order['order_accepted_timestamp']
                        print(f"\nCalculated duration from timestamps: {duration / 60:.1f} minutes")

                    # Sum up durations for a driver
                    driver_orders = [o for o in orders if o.get('driver_uuid') == orders[0]['driver_uuid']]
                    total_duration = 0

                    for order in driver_orders:
                        if order.get('order_accepted_timestamp') and order.get('order_finished_timestamp'):
                            duration = order['order_finished_timestamp'] - order['order_accepted_timestamp']
                            total_duration += duration

                    print(f"\nMETHOD 2 - Sum of ride durations: {total_duration / 3600:.2f} hours")
                    print(f"(Based on {len(driver_orders)} orders)")

    finally:
        await oauth_client.close()


if __name__ == "__main__":
    asyncio.run(debug_state_logs())