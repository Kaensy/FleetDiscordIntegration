import asyncio
import logging
from src.utils.config import load_config
from src.utils.logging import setup_logging
from src.bot.main import run_bot


async def main():
    """Main application entry point"""
    try:
        # Load configuration
        config = load_config()

        # Setup logging
        setup_logging(config.log_level)

        logger = logging.getLogger(__name__)
        logger.info("Starting Bolt Fleet Discord Bot...")

        # Run the bot
        await run_bot(config)

    except Exception as e:
        print(f"Application failed to start: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())