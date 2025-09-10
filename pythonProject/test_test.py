import sqlite3
from datetime import datetime, timedelta

conn = sqlite3.connect('data/fleet_data.db')
cursor = conn.cursor()

# Check what the bot might be querying
# The bot's date calculation for "month" view
start_date = datetime(2025, 8, 1, 0, 0, 0)
next_month = start_date + timedelta(days=32)  # This goes into September
end_date = next_month.replace(day=1)  # September 1st

start_ts = int(start_date.timestamp())
end_ts = int(end_date.timestamp())

print(f"Bot's likely query range:")
print(f"Start: {start_date} ({start_ts})")
print(f"End: {end_date} ({end_ts})")

# Query with the bot's likely timestamp range
cursor.execute("""
    SELECT COUNT(*) FROM orders 
    WHERE order_status = 'finished'
    AND order_finished_timestamp >= ?
    AND order_finished_timestamp < ?
""", (start_ts, end_ts))
print(f"Orders in this range: {cursor.fetchone()[0]}")

# Check if the bot might be using <= instead of
cursor.execute("""
    SELECT COUNT(*) FROM orders 
    WHERE order_status = 'finished'
    AND order_finished_timestamp >= ?
    AND order_finished_timestamp <= ?
""", (start_ts, end_ts))
print(f"Orders with <= end_date: {cursor.fetchone()[0]}")

# Check orders that might be excluded
cursor.execute("""
    SELECT 
        COUNT(*),
        MIN(datetime(order_finished_timestamp, 'unixepoch')),
        MAX(datetime(order_finished_timestamp, 'unixepoch'))
    FROM orders 
    WHERE order_status = 'finished'
    AND driver_uuid = 'd062347c-666e-49d1-ad99-63de3b9895a5'
    AND order_finished_timestamp >= ?
    AND order_finished_timestamp < ?
""", (start_ts, end_ts))
result = cursor.fetchone()
print(f"\nDriver-specific query:")
print(f"Count: {result[0]}, Min date: {result[1]}, Max date: {result[2]}")

# Check if there are orders with issues
cursor.execute("""
    SELECT COUNT(*) FROM orders 
    WHERE order_status = 'finished'
    AND driver_uuid = 'd062347c-666e-49d1-ad99-63de3b9895a5'
    AND order_finished_timestamp >= ?
    AND order_finished_timestamp < ?
    AND (ride_price IS NULL OR ride_price = 0)
""", (start_ts, end_ts))
print(f"Orders with NULL/0 price: {cursor.fetchone()[0]}")

conn.close()