import os
from dataclasses import dataclass
from typing import List, Optional
from dotenv import load_dotenv

load_dotenv()


@dataclass
class BoltConfig:
    client_id: str
    client_secret: str
    company_id: str
    token_url: str = "https://oidc.bolt.eu/token"
    scope: str = "fleet-integration:api"


@dataclass
class DiscordConfig:
    token: str
    guild_ids: List[int]
    command_prefix: str = "!"
    admin_user_ids: List[int] = None


@dataclass
class AppConfig:
    bolt: BoltConfig
    discord: DiscordConfig
    log_level: str = "INFO"
    update_interval_minutes: int = 30
    max_retries: int = 3
    database_url: str = "sqlite:///bot.db"


def load_config() -> AppConfig:
    """Load configuration from environment variables"""

    # Validate required environment variables
    required_vars = [
        'BOLT_CLIENT_ID', 'BOLT_CLIENT_SECRET', 'BOLT_COMPANY_ID',
        'DISCORD_TOKEN', 'DISCORD_GUILD_IDS'
    ]

    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {missing_vars}")

    bolt_config = BoltConfig(
        client_id=os.getenv('BOLT_CLIENT_ID'),
        client_secret=os.getenv('BOLT_CLIENT_SECRET'),
        company_id=os.getenv('BOLT_COMPANY_ID'),
        token_url=os.getenv('BOLT_TOKEN_URL', 'https://oidc.bolt.eu/token'),
        scope=os.getenv('BOLT_SCOPE', 'fleet-integration:api')
    )

    guild_ids = [int(id.strip()) for id in os.getenv('DISCORD_GUILD_IDS').split(',')]
    admin_user_ids = []
    if os.getenv('DISCORD_ADMIN_USER_IDS'):
        admin_user_ids = [int(id.strip()) for id in os.getenv('DISCORD_ADMIN_USER_IDS').split(',')]

    discord_config = DiscordConfig(
        token=os.getenv('DISCORD_TOKEN'),
        guild_ids=guild_ids,
        command_prefix=os.getenv('DISCORD_PREFIX', '!'),
        admin_user_ids=admin_user_ids
    )

    return AppConfig(
        bolt=bolt_config,
        discord=discord_config,
        log_level=os.getenv('LOG_LEVEL', 'INFO'),
        update_interval_minutes=int(os.getenv('UPDATE_INTERVAL_MINUTES', '30')),
        max_retries=int(os.getenv('MAX_RETRIES', '3')),
        database_url=os.getenv('DATABASE_URL', 'sqlite:///bot.db')
    )