import sqlite3
from pathlib import Path

db_path = Path("fleet_data.db")
with sqlite3.connect(db_path) as conn:
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(orders)")
    columns = [column[1] for column in cursor.fetchall()]

    if 'order_accepted_timestamp' not in columns:
        cursor.execute('ALTER TABLE orders ADD COLUMN order_accepted_timestamp INTEGER')
        conn.commit()
        print("✅ Column added!")

print("✅ Migration complete!")