from pathlib import Path
import logging
from enum import Enum
import os
from requests_cache import FileCache
import requests_cache

DEBUG = os.getenv('DEBUG') or 1

###### Paths #######

ROOT_DIR = Path(__file__).parent.parent.resolve()
DATA_DIR = Path(ROOT_DIR / 'data')

INPUT_DIR = Path(DATA_DIR / 'input')
INTERMEDIATE_DIR = Path(DATA_DIR / 'intermediate')
OUTPUT_DIR = Path(DATA_DIR / 'output')

INTERMEDIATE_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

CACHE_DIR = Path(ROOT_DIR / '.cache')
CACHE_DIR.mkdir(parents=True, exist_ok=True)

LOG_DIR = Path(ROOT_DIR / '.log')
LOG_DIR.mkdir(parents=True, exist_ok=True)

# Requests cache
# file_cache_dir=FileCache(use_cache_dir=CACHE_DIR)
# requests_cache.install_cache('pkb', backend=file_cache_dir)

# # Directory to save logs and model checkpoints, if not provided
# # through the command line argument --logs
# DEFAULT_LOGS_DIR = DATA_DIR / "logs"

###### Logging #######

# Set up logging - inherit from luigi so we use the same interface
logger = logging.getLogger('luigi-interface')

# Capture all log levels, but handlers below set their own levels
logger.setLevel(logging.DEBUG)

# Set up file logging for errors and warnings
file_handler = logging.FileHandler(LOG_DIR / 'error.log')
file_handler.setFormatter(
    logging.Formatter("[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s")
)
# Log errors to files
file_handler.setLevel(logging.WARNING)
logger.addHandler(file_handler)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
logger.addHandler(console_handler)







