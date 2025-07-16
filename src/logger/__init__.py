# app/logger.py

import logging
import os
import sys
from logging.handlers import RotatingFileHandler
from datetime import datetime

# Constants
LOG_DIR = 'logs'
MAX_LOG_SIZE = 5 * 1024 * 1024  # 5 MB
BACKUP_COUNT = 3  # Rotate up to 3 old logs

# Ensure log directory exists
root_dir = os.path.dirname(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))
log_dir_path = os.path.join(root_dir, LOG_DIR)
os.makedirs(log_dir_path, exist_ok=True)

# Create log file with timestamp
timestamp = datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
log_file_path = os.path.join(log_dir_path, f"{timestamp}.log")

def get_logger(name: str) -> logging.Logger:
    """
    Returns a configured logger with RotatingFileHandler and StreamHandler.
    Ensures that each module gets its own named logger.
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    if not logger.handlers:
        formatter = logging.Formatter("[ %(asctime)s ] %(name)s - %(levelname)s - %(message)s")

        # File handler
        file_handler = RotatingFileHandler(
            log_file_path, maxBytes=MAX_LOG_SIZE, backupCount=BACKUP_COUNT, encoding="utf-8"
        )
        file_handler.setFormatter(formatter)
        file_handler.setLevel(logging.INFO)

        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        console_handler.setLevel(logging.INFO)

        # Attach handlers
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger
