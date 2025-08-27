# health_check.py - Run this to verify everything is working
import asyncio
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()


async def check_health():
    """Comprehensive health check for the bot"""

    print("=" * 60)
    print("BOLT FLEET DISCORD BOT - HEALTH CHECK")
    print("=" * 60)

    checks_passed = []
    checks_failed = []

    # 1. Check environment variables
    print("\n1. ENVIRONMENT VARIABLES:")
    required_vars = [
        'BOLT_CLIENT_ID',
        'BOLT_CLIENT_SECRET',
        'BOLT_COMPANY_ID',
        'DISCORD_TOKEN',
        'DISCORD_GUILD_IDS'
    ]

    for var in required_vars:
        value = os.getenv(var)
        if value:
            print(f"  ✅ {var}: {'*' * 10} (set)")
            checks_passed.append(f"{var} configured")
        else:
            print(f"  ❌ {var}: NOT SET")
            checks_failed.append(f"{var} missing")

    # 2. Check OAuth token
    print("\n2. OAUTH TOKEN:")
    token_file = Path("oauth_token.json")
    if token_file.exists():
        import json
        with open(token_file) as f:
            token_data = json.load(f)

        if 'expires_at' in token_data:
            expires_at = datetime.fromtimestamp(token_data['expires_at'])
            if expires_at > datetime.now():
                print(f"  ✅ Token valid until: {expires_at}")
                checks_passed.append("OAuth token valid")
            else:
                print(f"  ⚠️ Token expired at: {expires_at}")
                checks_failed.append("OAuth token expired")
    else:
        print("  ℹ️ No token file - will be created on first run")

    # 3. Check database
    print("\n3. DATABASE:")
    db_path = Path("data/fleet_data.db")
    if not db_path.parent.exists():
        db_path.parent.mkdir(exist_ok=True)

    if db_path.exists():
        import sqlite3
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Check tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        print(f"  ✅ Database exists with {len(tables)} tables")

        # Check orders
        cursor.execute("SELECT COUNT(*) FROM orders")
        order_count = cursor.fetchone()[0]
        print(f"  ℹ️ Total orders: {order_count}")

        # Check recent sync
        cursor.execute("""
            SELECT MAX(synced_at) FROM orders
        """)
        last_sync = cursor.fetchone()[0]
        if last_sync:
            print(f"  ℹ️ Last sync: {last_sync}")

        conn.close()
        checks_passed.append(f"Database healthy ({order_count} orders)")
    else:
        print("  ℹ️ No database - will be created on first run")

    # 4. Test Bolt API connection
    print("\n4. BOLT API CONNECTION:")
    if all(os.getenv(var) for var in ['BOLT_CLIENT_ID', 'BOLT_CLIENT_SECRET']):
        try:
            from src.oauth.client import BoltOAuthClient

            oauth = BoltOAuthClient(
                client_id=os.getenv('BOLT_CLIENT_ID'),
                client_secret=os.getenv('BOLT_CLIENT_SECRET')
            )

            token = await oauth.get_valid_token()
            print(f"  ✅ API authentication successful")
            checks_passed.append("Bolt API connected")

            # Test company access
            company_id = os.getenv('BOLT_COMPANY_ID')
            if company_id:
                response = await oauth.make_request(
                    None, 'GET',
                    "https://node.bolt.eu/fleet-integration-gateway/fleetIntegration/v1/getCompanies"
                )

                if response.get('code') == 0:
                    companies = response.get('data', {}).get('company_ids', [])
                    if int(company_id) in companies:
                        print(f"  ✅ Company {company_id} access confirmed")
                        checks_passed.append("Company access verified")
                    else:
                        print(f"  ❌ Company {company_id} not in accessible companies: {companies}")
                        checks_failed.append("Company access denied")

            await oauth.close()

        except Exception as e:
            print(f"  ❌ API Error: {e}")
            checks_failed.append(f"API error: {str(e)[:50]}")
    else:
        print("  ⚠️ Cannot test - credentials not set")

    # 5. Check Discord bot
    print("\n5. DISCORD BOT:")
    if os.getenv('DISCORD_TOKEN'):
        print("  ✅ Discord token configured")
        checks_passed.append("Discord token set")

        guild_ids = os.getenv('DISCORD_GUILD_IDS', '').split(',')
        print(f"  ℹ️ Configured for {len(guild_ids)} guild(s): {', '.join(guild_ids)}")
    else:
        print("  ❌ Discord token not configured")
        checks_failed.append("Discord token missing")

    # 6. Check Python dependencies
    print("\n6. PYTHON DEPENDENCIES:")
    required_packages = [
        'discord', 'aiohttp', 'dotenv', 'apscheduler', 'cryptography', 'psutil'
    ]

    for package in required_packages:
        try:
            __import__(package)
            print(f"  ✅ {package}")
            checks_passed.append(f"{package} installed")
        except ImportError:
            print(f"  ❌ {package} - NOT INSTALLED")
            checks_failed.append(f"{package} missing")

    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY:")
    print(f"  ✅ Passed: {len(checks_passed)} checks")
    print(f"  ❌ Failed: {len(checks_failed)} checks")

    if checks_failed:
        print("\n⚠️ ISSUES TO FIX:")
        for issue in checks_failed:
            print(f"  - {issue}")
        return False
    else:
        print("\n🎉 ALL CHECKS PASSED - Bot ready to run!")
        return True


if __name__ == "__main__":
    result = asyncio.run(check_health())
    sys.exit(0 if result else 1)