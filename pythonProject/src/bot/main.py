import discord
from discord.ext import commands
import logging
import asyncio
from ..oauth.client_requests import BoltOAuthClient
from ..api.bolt_client import BoltFleetClient
from ..utils.config import AppConfig

logger = logging.getLogger(__name__)


class BoltFleetBot(commands.Bot):
    """Main Discord bot class with Bolt Fleet API integration"""

    def __init__(self, config: AppConfig):
        intents = discord.Intents.default()
        intents.message_content = True  # Required for text commands
        intents.guilds = True  # Required for guild information
        intents.members = True  # Optional but useful

        super().__init__(
            command_prefix=config.discord.command_prefix,
            intents=intents,
            help_command=None
        )

        self.config = config

        # Initialize OAuth client
        self.oauth_client = BoltOAuthClient(
            client_id=config.bolt.client_id,
            client_secret=config.bolt.client_secret,
            token_url=config.bolt.token_url,
            scope=config.bolt.scope
        )

        # Initialize Bolt Fleet client
        self.bolt_client = BoltFleetClient(self.oauth_client, company_id=config.bolt.company_id)

    async def setup_hook(self):
        """Setup bot and load extensions"""
        logger.info("Setting up bot...")

        # Load cogs
        extensions = [
            'src.bot.cogs.fleet',
            'src.bot.cogs.scheduler'
        ]

        for extension in extensions:
            try:
                await self.load_extension(extension)
                logger.info(f"Loaded extension: {extension}")
            except Exception as e:
                logger.error(f"Failed to load extension {extension}: {e}")

        # Sync slash commands
        try:
            if self.config.discord.guild_ids:
                for guild_id in self.config.discord.guild_ids:
                    try:
                        guild = discord.Object(id=guild_id)
                        synced = await self.tree.sync(guild=guild)
                        logger.info(f"Synced {len(synced)} commands to guild {guild_id}")
                    except discord.errors.Forbidden:
                        logger.warning(
                            f"Missing permissions to sync commands in guild {guild_id}. Make sure the bot is added to this server.")
                    except Exception as e:
                        logger.error(f"Failed to sync commands to guild {guild_id}: {e}")
            else:
                # Sync globally (takes up to 1 hour to propagate)
                synced = await self.tree.sync()
                logger.info(f"Synced {len(synced)} global commands")
        except Exception as e:
            logger.error(f"Failed to sync commands: {e}")

        logger.info("Bot setup complete")

    async def on_ready(self):
        """Called when bot is ready"""
        logger.info(f"{self.user} has connected to Discord!")
        logger.info(f"Bot is in {len(self.guilds)} guilds")

        # List all guilds the bot is in
        for guild in self.guilds:
            logger.info(f"  - {guild.name} (ID: {guild.id})")

        # Set bot status
        activity = discord.Activity(
            type=discord.ActivityType.watching,
            name="Bolt Fleet Data"
        )
        await self.change_presence(activity=activity)

    async def on_guild_join(self, guild):
        """Called when the bot joins a new guild"""
        logger.info(f"Bot joined new guild: {guild.name} (ID: {guild.id})")

        # Try to sync commands to the new guild
        try:
            synced = await self.tree.sync(guild=guild)
            logger.info(f"Synced {len(synced)} commands to new guild {guild.id}")
        except Exception as e:
            logger.error(f"Failed to sync commands to new guild {guild.id}: {e}")

    async def on_command_error(self, ctx, error):
        """Global command error handler"""
        if isinstance(error, commands.CommandNotFound):
            return  # Ignore unknown commands

        elif isinstance(error, commands.MissingPermissions):
            await ctx.send("❌ You don't have permission to use this command.")

        elif isinstance(error, commands.MissingRequiredArgument):
            await ctx.send(f"❌ Missing required argument: `{error.param.name}`")

        elif isinstance(error, commands.BadArgument):
            await ctx.send("❌ Invalid argument provided.")

        elif isinstance(error, commands.CommandOnCooldown):
            await ctx.send(f"❌ Command on cooldown. Try again in {error.retry_after:.1f}s")

        else:
            logger.error(f"Unhandled error in command {ctx.command}: {error}", exc_info=True)
            await ctx.send("❌ An unexpected error occurred. Please try again later.")

    async def close(self):
        """Clean up resources when bot shuts down"""
        logger.info("Shutting down bot...")
        await self.oauth_client.close()
        await super().close()


async def run_bot(config: AppConfig):
    """Run the Discord bot"""
    bot = BoltFleetBot(config)

    try:
        async with bot:
            await bot.start(config.discord.token)
    except discord.errors.PrivilegedIntentsRequired:
        logger.error(
            "Bot requires privileged intents! Please enable them in Discord Developer Portal:\n"
            "1. Go to https://discord.com/developers/applications/\n"
            "2. Select your application\n"
            "3. Go to 'Bot' section\n"
            "4. Enable 'MESSAGE CONTENT INTENT' and 'SERVER MEMBERS INTENT'\n"
            "5. Save changes and restart the bot"
        )
    except discord.errors.LoginFailure:
        logger.error(
            "Invalid bot token! Please check your DISCORD_TOKEN in the .env file"
        )
    except KeyboardInterrupt:
        logger.info("Bot shutdown requested")
    except Exception as e:
        logger.error(f"Bot error: {e}", exc_info=True)
    finally:
        await bot.close()