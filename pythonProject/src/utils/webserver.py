from aiohttp import web
import asyncio
import logging
import os

logger = logging.getLogger(__name__)


async def health_check(request):
    """Health check endpoint"""
    return web.json_response({
        "status": "healthy",
        "bot": "Bolt Fleet Discord Bot",
        "timestamp": asyncio.get_event_loop().time()
    })


async def keep_alive_ping(request):
    """Keep-alive endpoint to prevent Render from sleeping"""
    return web.json_response({"status": "awake"})


async def start_web_server():
    """Start web server for health checks and keep-alive"""
    app = web.Application()
    app.router.add_get('/health', health_check)
    app.router.add_get('/ping', keep_alive_ping)
    app.router.add_get('/', health_check)  # Root endpoint

    # Use Render's PORT environment variable
    port = int(os.environ.get('PORT', 8080))

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    logger.info(f"Web server started on port {port}")
    return runner