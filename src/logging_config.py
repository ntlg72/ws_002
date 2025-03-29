# logging_config.py (Place this module at the root of your ws_002 project)

import logging
import os

def setup_logging(log_level=logging.INFO, log_file=None, log_format='%(asctime)s - %(levelname)s - %(name)s - %(message)s'):
    """
    Configures logging for the entire project.

    Args:
        log_level (int): The logging level (e.g., logging.INFO, logging.DEBUG).
        log_file (str, optional): The path to a log file. If None, logs are output to the console.
        log_format (str): The format string for log messages.
    """

    # Get the root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    # Remove any existing handlers to avoid duplicate logging
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    formatter = logging.Formatter(log_format)

    if log_file:
        # Create the log file directory if it doesn't exist
        log_dir = os.path.dirname(log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir)

        handler = logging.FileHandler(log_file)
    else:
        handler = logging.StreamHandler()

    handler.setFormatter(formatter)
    root_logger.addHandler(handler)