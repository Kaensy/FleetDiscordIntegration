import asyncio
import logging
import os
from src.utils.config import load_config
from src.utils.logging import setup_logging
from src.utils.webserver import start_web_server
from src.bot.main import run_bot


async def keep_render_awake():
    """Ping self every 14 minutes to prevent sleeping"""
    await asyncio.sleep(300)  # Wait 5 minutes before starting pings

    render_url = os.environ.get('RENDER_EXTERNAL_URL')
    if not render_url:
        logger.info("Not running on Render, skipping keep-alive")
        return

    import aiohttp
    while True:
        await asyncio.sleep(840)  # 14 minutes
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{render_url}/ping", timeout=10) as response:
                    if response.status == 200:
                        logger.debug("Keep-alive ping successful")
        except Exception as e:
            logger.warning(f"Keep-alive ping failed: {e}")


async def main():
    """Main application entry point"""
    try:
        # Load configuration
        config = load_config()

        # Setup logging
        setup_logging(config.log_level)

        logger = logging.getLogger(__name__)
        logger.info("Starting Bolt Fleet Discord Bot...")

        # Start web server for Render
        await start_web_server()

        # Start keep-alive task
        asyncio.create_task(keep_render_awake())

        # Run the bot
        await run_bot(config)

    except Exception as e:
        print(f"Application failed to start: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())