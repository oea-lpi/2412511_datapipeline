import logging
import os
import re
import threading

import redis

from helper.redis_utility import start_heartbeat
from logger.setup_logging import setup_logging
from scripts.Pipeline import Pipeline


logger = logging.getLogger("conv_sens")

pattern = os.getenv("SENS_PATTERN", "(\d{4}-\d{2}-\d{2})_(\d{2}-\d{2}-\d{2})")
SENS_RE = re.compile(pattern)
HEALTH_CONTAINER_CONV_SENS = os.getenv("HEALTH_CONTAINER_CONV_SENS", "health:container_conv_sens")

def main():
    setup_logging(process_name="conv_sens")

    redis_db = redis.Redis(
        host=os.getenv("REDIS_HOST","redis"),
        port=int(os.getenv("REDIS_PORT",6379)),
        db=int(os.getenv("REDIS_DB",0)),
        decode_responses=True
    )

    start_heartbeat(redis_client=redis_db, key=HEALTH_CONTAINER_CONV_SENS)

    pipelines = [
        Pipeline(
            name        = "sens",
            input_dir   = os.getenv("INPUT_DIR",  "/app/files/input"),
            failed_dir  = os.getenv("FAILED_DIR", "/app/files/failed"),
            stats_dir   = os.getenv("STATS_DIR",  "/app/files/stats"),
            finished_dir= os.getenv("FINISHED_DIR", "/app/files/finished"),
            timestamp_re= SENS_RE,
            datetime_fmt= "%Y-%m-%d %H-%M-%S",
            redis_db    = redis_db
        )
    ]

    for p in pipelines:
        logger.info(f"Started pipeline {p.name} watching {p.input}.")
    threading.Event().wait()

if __name__ == "__main__":
    main()
