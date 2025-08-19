import logging
import logging.handlers
from pathlib import Path


def setup_logging(level: str = "INFO"):
    """Setup comprehensive logging configuration"""

    # Create logs directory
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    # Configure root logger
    logging.getLogger().setLevel(getattr(logging, level.upper()))

    # Create formatters
    detailed_formatter = logging.Formatter(
        '[{asctime}] [{levelname:<8}] {name}: {message}',
        '%Y-%m-%d %H:%M:%S',
        style='{'
    )

    simple_formatter = logging.Formatter(
        '{levelname}: {message}',
        style='{'
    )

    # File handler with rotation
    file_handler = logging.handlers.RotatingFileHandler(
        log_dir / "bot.log",
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setFormatter(detailed_formatter)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(simple_formatter)

    # Configure specific loggers
    loggers = [
        'src',
        'discord',
        'aiohttp'
    ]

    for logger_name in loggers:
        logger = logging.getLogger(logger_name)
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        logger.setLevel(getattr(logging, level.upper()))

    # Suppress noisy loggers
    logging.getLogger('discord.http').setLevel(logging.WARNING)
    logging.getLogger('aiohttp.access').setLevel(logging.WARNING)