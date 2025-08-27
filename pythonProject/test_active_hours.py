# test_active_hours.py - Test the fixed hours calculation
import asyncio
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
from src.oauth.client import BoltOAuthClient
from src.api.bolt_client import BoltFleetClient

load_dotenv()


async def test_active_hours():
    """Test the active hours calculation with real data"""

    oauth_client = BoltOAuthClient(
        client_id=os.getenv('BOLT_CLIENT_ID'),
        client_secret=os.getenv('BOLT_CLIENT_SECRET')
    )

    fleet_client = BoltFleetClient(oauth_client, company_id=os.getenv('BOLT_COMPANY_ID'))

    try:
        async with fleet_client:
            # Test for last 2 days
            start_date = datetime.now() - timedelta(days=2)
            end_date = datetime.now()

            print("=" * 60)
            print(f"TESTING ACTIVE HOURS CALCULATION")
            print(f"Period: {start_date} to {end_date}")
            print("=" * 60)

            # Fetch state logs
            state_response = await fleet_client.get_fleet_state_logs(
                start_date=start_date,
                end_date=end_date,
                limit=1000
            )

            if state_response.get('code') == 0:
                state_logs = state_response.get('data', {}).get('state_logs', [])
                print(f"\n✅ Fetched {len(state_logs)} state logs")

                # Group by driver
                drivers = {}
                for log in state_logs:
                    driver_uuid = log.get('driver_uuid')
                    if driver_uuid not in drivers:
                        drivers[driver_uuid] = []
                    drivers[driver_uuid].append(log)

                # Test calculation for each driver
                for driver_uuid, logs in drivers.items():
                    print(f"\n{'=' * 40}")
                    print(f"Driver: {driver_uuid[:8]}...")
                    print(f"State logs: {len(logs)}")

                    # Show state transitions
                    logs.sort(key=lambda x: x.get('created', 0))

                    print("\nState transitions:")
                    online_periods = []
                    online_start = None

                    for log in logs:
                        state = log.get('state', '')
                        timestamp = log.get('created', 0)
                        dt = datetime.fromtimestamp(timestamp)

                        print(f"  {dt.strftime('%Y-%m-%d %H:%M')}: {state}")

                        # Track online periods
                        if state in ['waiting_orders', 'has_order']:
                            if online_start is None:
                                online_start = timestamp
                        elif state == 'inactive' and online_start:
                            duration = (timestamp - online_start) / 3600
                            online_periods.append(duration)
                            print(f"    └─ Online period: {duration:.2f} hours")
                            online_start = None

                    # If still online
                    if online_start:
                        duration = (end_date.timestamp() - online_start) / 3600
                        online_periods.append(duration)
                        print(f"    └─ Still online: {duration:.2f} hours")

                    total_hours = sum(online_periods)
                    print(f"\n📊 RESULT:")
                    print(f"  Online periods: {len(online_periods)}")
                    print(f"  Total active hours: {total_hours:.2f}")

                    # Now get stats from database for comparison
                    db_stats = fleet_client.db.get_driver_stats_by_uuid(
                        driver_uuid,
                        days=2,
                        state_logs=logs
                    )

                    if db_stats:
                        print(f"\n📈 Database Stats:")
                        print(f"  Orders: {db_stats['orders_completed']}")
                        print(f"  Active Hours (calculated): {db_stats['hours_worked']}")
                        print(f"  Gross Earnings: {db_stats['gross_earnings']} RON")
                        print(f"  Earnings/Hour: {db_stats['earnings_per_hour']} RON/hr")
            else:
                print(f"❌ Failed to fetch state logs: {state_response}")

    finally:
        await oauth_client.close()


if __name__ == "__main__":
    asyncio.run(test_active_hours())