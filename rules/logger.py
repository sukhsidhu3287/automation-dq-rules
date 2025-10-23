import logging
import sys
from datetime import datetime

# Create logs directory if it doesn't exist
import os
LOGS_DIR = "logs"
os.makedirs(LOGS_DIR, exist_ok=True)

# Single log file that gets replaced on each action
LOG_FILE = os.path.join(LOGS_DIR, "automation.log")

def reset_log_file():
    global LOG_FILE
    # Delete the old log file if it exists
    if os.path.exists(LOG_FILE):
        try:
            os.remove(LOG_FILE)
        except Exception:
            pass  # If file is in use, just overwrite it


class ColoredConsoleFormatter(logging.Formatter):    
    def format(self, record):
        # Keep the original message with emojis
        return f"{self.formatTime(record)} - {record.levelname:8s} - {record.getMessage()}"


class FileFormatter(logging.Formatter):    
    def format(self, record):
        return f"{self.formatTime(record)} - {record.levelname:8s} - {record.getMessage()}"


def setup_logger(name="dq_automation"):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    
    # Clear any existing handlers
    if logger.hasHandlers():
        logger.handlers.clear()
    
    # Console handler (with colors/emojis)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_formatter = ColoredConsoleFormatter(
        fmt='%(asctime)s - %(levelname)-8s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_handler.setFormatter(console_formatter)
    
    # File handler (write mode - new file each session, clean formatting)
    file_handler = logging.FileHandler(LOG_FILE, mode='w', encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_formatter = FileFormatter(
        fmt='%(asctime)s - %(levelname)-8s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(file_formatter)
    
    # Add handlers
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    
    return logger


def log_separator(logger, char="=", length=70):
    logger.info(char * length)


def log_section_start(logger, title):
    logger.info("")
    log_separator(logger, "=", 70)
    logger.info(title.upper())
    log_separator(logger, "=", 70)


def log_section_end(logger, title):
    log_separator(logger, "=", 70)
    logger.info(f"✅ {title.upper()} COMPLETE")
    log_separator(logger, "=", 70)
    logger.info("")


def log_subsection(logger, title):
    logger.info("")
    logger.info("─" * 60)
    logger.info(title)
    logger.info("─" * 60)


def log_file_operation(logger, operation, filepath):
    logger.info(f"📄 {operation}: {filepath}")


def log_error(logger, message, exception=None):
    logger.error(f"❌ {message}")
    if exception:
        logger.exception(exception)


def log_warning(logger, message):
    logger.warning(f"⚠️  {message}")


def log_success(logger, message):
    logger.info(f"✅ {message}")


def log_info(logger, message):
    logger.info(f"ℹ️  {message}")


def log_processing(logger, item):
    logger.info(f"🔄 Processing: {item}")


# Create default logger instance
default_logger = setup_logger()


# Convenience functions for direct use
def info(message):
    default_logger.info(message)


def warning(message):
    log_warning(default_logger, message)


def error(message, exception=None):
    log_error(default_logger, message, exception)


def success(message):
    log_success(default_logger, message)


def debug(message):
    default_logger.debug(message)

