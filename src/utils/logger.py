"""
Structured logging configuration for the ETL pipeline
"""
import sys
import os
from pathlib import Path
from loguru import logger
from datetime import datetime


def setup_logger(
    log_level: str = "INFO",
    log_file: str = None,
    rotation: str = "1 day",
    retention: str = "30 days",
    format_type: str = "json"
):
    """
    Configure loguru logger with structured logging
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Path to log file (if None, logs only to console)
        rotation: When to rotate log file
        retention: How long to keep old log files
        format_type: 'json' for structured logs or 'text' for human-readable
    """
    # Remove default handler
    logger.remove()
    
    # Console handler with colors
    if format_type == "json":
        console_format = (
            "{time:YYYY-MM-DD HH:mm:ss} | "
            "{level: <8} | "
            "{name}:{function}:{line} | "
            "{message}"
        )
    else:
        console_format = (
            "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
            "<level>{level: <8}</level> | "
            "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
            "<level>{message}</level>"
        )
    
    logger.add(
        sys.stdout,
        format=console_format,
        level=log_level,
        colorize=True if format_type == "text" else False
    )
    
    # File handler with rotation
    if log_file:
        # Create logs directory if it doesn't exist
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        if format_type == "json":
            # JSON format for structured logging
            logger.add(
                log_file,
                format="{message}",
                level=log_level,
                rotation=rotation,
                retention=retention,
                compression="zip",
                serialize=True  # This makes it output JSON
            )
        else:
            logger.add(
                log_file,
                format=console_format,
                level=log_level,
                rotation=rotation,
                retention=retention,
                compression="zip"
            )
    
    return logger


def log_execution_time(func):
    """
    Decorator to log function execution time
    """
    def wrapper(*args, **kwargs):
        start_time = datetime.now()
        logger.info(f"Starting execution: {func.__name__}")
        
        try:
            result = func(*args, **kwargs)
            execution_time = (datetime.now() - start_time).total_seconds()
            logger.info(
                f"Completed: {func.__name__}",
                extra={
                    "execution_time_seconds": execution_time,
                    "function": func.__name__
                }
            )
            return result
        except Exception as e:
            execution_time = (datetime.now() - start_time).total_seconds()
            logger.error(
                f"Failed: {func.__name__}",
                extra={
                    "execution_time_seconds": execution_time,
                    "function": func.__name__,
                    "error": str(e)
                }
            )
            raise
    
    return wrapper


# Initialize default logger
_log_level = os.getenv("LOG_LEVEL", "INFO")
_log_file = os.getenv("LOG_FILE", "logs/etl_pipeline.log")

setup_logger(
    log_level=_log_level,
    log_file=_log_file,
    format_type="text"  # Use 'json' for production
)