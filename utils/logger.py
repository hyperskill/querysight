import logging
import sys
import os
from datetime import datetime
from typing import Optional
from pathlib import Path

class CustomFormatter(logging.Formatter):
    """Custom formatter with colors for different log levels"""
    
    COLORS = {
        'DEBUG': '\033[0;36m',    # Cyan
        'INFO': '\033[0;32m',     # Green
        'WARNING': '\033[0;33m',  # Yellow
        'ERROR': '\033[0;31m',    # Red
        'CRITICAL': '\033[0;35m', # Purple
        'RESET': '\033[0m'        # Reset
    }

    def format(self, record):
        # Save the original format
        format_orig = self._style._fmt

        # Add colors if it's going to the terminal
        if sys.stderr.isatty():
            self._style._fmt = (f"{self.COLORS.get(record.levelname, self.COLORS['RESET'])}"
                              f"%(asctime)s | %(processName)s:%(process)d | "
                              f"%(levelname)s | %(name)s | "
                              f"%(message)s{self.COLORS['RESET']}")
        else:
            self._style._fmt = ("%(asctime)s | %(processName)s:%(process)d | "
                              "%(levelname)s | %(name)s | %(message)s")

        # Call the original formatter class to do the grunt work
        result = logging.Formatter.format(self, record)

        # Restore the original format
        self._style._fmt = format_orig

        return result

def setup_logger(name: str, log_level: str = "INFO", log_file: Optional[str] = None) -> logging.Logger:
    """
    Set up a logger with both console and file handlers
    
    Args:
        name: The name of the logger
        log_level: The minimum log level to capture
        log_file: Optional file path for logging. If None, only console logging is used
    
    Returns:
        logging.Logger: Configured logger instance
    """
    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, log_level.upper()))

    # Prevent adding handlers if they already exist
    if logger.handlers:
        return logger

    # Create console handler with custom formatter
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(CustomFormatter())
    logger.addHandler(console_handler)

    # Create file handler if log_file is specified
    if log_file:
        # Ensure log directory exists
        log_dir = os.path.dirname(log_file)
        if log_dir:
            Path(log_dir).mkdir(parents=True, exist_ok=True)

        # Create file handler
        file_handler = logging.FileHandler(log_file)
        file_formatter = logging.Formatter(
            '%(asctime)s | %(processName)s:%(process)d | %(levelname)s | '
            '%(name)s | %(message)s'
        )
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)

    return logger

# Create logs directory
LOGS_DIR = "logs"
Path(LOGS_DIR).mkdir(exist_ok=True)

# Default logger setup
default_logger = setup_logger(
    "querysight",
    log_level="DEBUG",
    log_file=f"logs/querysight_{datetime.now().strftime('%Y%m%d')}.log"
)
