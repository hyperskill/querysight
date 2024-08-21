import logging
import os
from datetime import datetime

def setup_logger(log_dir='logs'):
    # Create log directory if it doesn't exist
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # Create log filename with current date and time
    current_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_file = os.path.join(log_dir, f'querysight_{current_time}.log')

    # Configure the logger
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()  # This will also show logs in the console
        ]
    )

    return logging.getLogger(__name__)

def get_latest_log_file(log_dir='logs'):
    if not os.path.exists(log_dir):
        return None
    log_files = [f for f in os.listdir(log_dir) if f.endswith('.log')]
    if not log_files:
        return None
    latest_log = max(log_files, key=lambda f: os.path.getmtime(os.path.join(log_dir, f)))
    return os.path.join(log_dir, latest_log)

# Create global logger
logger = setup_logger()
