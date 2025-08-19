# test_setup.py
import sys
import os
from pathlib import Path

print("Python version:", sys.version)
print("Project path:", Path.cwd())

# Test imports
try:
    from dotenv import load_dotenv
    print("✓ python-dotenv installed")
except ImportError:
    print("✗ python-dotenv NOT installed")

try:
    import discord
    print("✓ discord.py installed")
except ImportError:
    print("✗ discord.py NOT installed")

try:
    import aiohttp
    print("✓ aiohttp installed")
except ImportError:
    print("✗ aiohttp NOT installed")

try:
    import authlib
    print("✓ authlib installed")
except ImportError:
    print("✗ authlib NOT installed")

# Test .env file
load_dotenv()
print("\nEnvironment Variables:")
print("Bolt Client ID:", "✓ Set" if os.getenv('BOLT_CLIENT_ID') else "✗ Not set")
print("Bolt Secret:", "✓ Set" if os.getenv('BOLT_CLIENT_SECRET') else "✗ Not set")
print("Discord Token:", "✓ Set" if os.getenv('DISCORD_TOKEN') else "✗ Not set")
print("Guild IDs:", os.getenv('DISCORD_GUILD_IDS') or "✗ Not set")

# Test project structure
print("\nProject Structure:")
dirs = ['src', 'src/oauth', 'src/api', 'src/bot', 'src/bot/cogs', 'src/utils']
for dir in dirs:
    if Path(dir).exists():
        print(f"✓ {dir}/ exists")
    else:
        print(f"✗ {dir}/ missing")