import logging
from pathlib import Path

import pandas as pd
import redis

from helper.processing import move_to_finished


logger = logging.getLogger(__name__)

def check_readability(file_path: Path):
    """
    Simple check for file existence and filetype being .csv.

    Args:
        file_path: Path object to the currently to be processed file. 
    """
    if not file_path.is_file():
        logger.error(f"File not found: {file_path}.")
        return
    
    if file_path.suffix.lower() != ".csv":
        logger.error(f"Called on non-.csv file: {file_path}.")
        return

def file_analysis(file_path: Path): ...

def redis_push(redis_db: redis.Redis): ...

def main(file_path: Path, finished_dir: Path, redis_db: redis.Redis):
    check_readability(file_path)
    file_analysis(file_path)
    redis_push(redis_db)
    move_to_finished(file_path, finished_dir)