import logging
import os
import re
import threading

import redis

from logger.setup_logging import setup_logging
from scripts.Pipeline import Pipeline


logger = logging.getLogger("conv")

LPI_RE = re.compile(r'(\d{4}-\d{2}-\d{2})_(\d{2}-\d{2}-\d{2})')

def main():
    setup_logging(process_name="conv")

    redis_db = redis.Redis(
        host=os.getenv("REDIS_HOST","redis"),
        port=int(os.getenv("REDIS_PORT",6379)),
        db=int(os.getenv("REDIS_DB",0)),
        decode_responses=True
    )

    pipelines = [
        Pipeline(
            name        = "Sensical",
            input_dir   = os.getenv("INPUT_DIR_SENS",  "/app/files/input_sens"),
            failed_dir  = os.getenv("FAILED_DIR_SENS", "/app/files/failed_sens"),
            stats_dir   = os.getenv("STATS_DIR_SENS",  "/app/files/stats_sens"),
            finished_dir= os.getenv("FINISHED_DIR_SENS", "/app/files/finished_sens"),
            timestamp_re= LPI_RE,
            datetime_fmt= "%Y-%m-%d %H-%M-%S",
            redis_db    = redis_db
        )
    ]

    for p in pipelines:
        logger.info(f"Started pipeline {p.name} watching {p.input}.")
    threading.Event().wait()

if __name__=="__main__":
    main()
