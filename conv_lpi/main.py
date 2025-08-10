import atexit
from datetime import datetime
import json
import logging.config
import os
from pathlib import Path
from queue import Queue
import re
import threading
import time
import shutil

import redis
from watchdog.observers.polling import PollingObserver

from helper.redis_utility import start_heartbeat
from logger.setup_logging import setup_logging
from scripts.Pipeline import Pipeline


logger = logging.getLogger("conv_lpi")

TIMESTAMP_RE = re.compile(r'(\d{4}-\d{2}-\d{2})_(\d{2}-\d{2}-\d{2})')

def main():
    setup_logging()

    redis_db = redis.Redis(
        host=os.getenv("REDIS_HOST","redis"),
        port=int(os.getenv("REDIS_PORT",6379)),
        db=int(os.getenv("REDIS_DB",0)),
        decode_responses=True,
    )

    # Start heartbeat thread before pipelines come up.
    start_heartbeat(
      redis_client=redis_db,
      key=os.getenv("HEARTBEAT_KEY_CONVERTER","heartbeat_converter"),
      interval=int(os.getenv("HEARTBEAT_INTERVAL",20)),
      ttl=int(os.getenv("HEARTBEAT_TTL",60)),
    )
    logger.debug("Heartbeat thread for the converter started.")

    pipelines = [
        Pipeline(
            name       = "10Hz",
            input_dir  = os.getenv("INPUT_DIR_10HZ",  "/app/files/input"),
            failed_dir = os.getenv("FAILED_DIR_10HZ", "/app/files/failed"),
            stats_dir  = os.getenv("STATS_DIR_10HZ",  "/app/files/stats"),
            finished_dir=os.getenv("FINISHED_DIR_10HZ", "/app/files/finished"),
            timestamp_re=TIMESTAMP_RE,
            datetime_fmt="%Y-%m-%d %H-%M-%S",
            redis_db    = redis_db,
        ),
        Pipeline(
            name       = "100Hz",
            input_dir  = os.getenv("INPUT_DIR_100HZ",  "/app/files/input_100hz"),
            failed_dir = os.getenv("FAILED_DIR_100HZ", "/app/files/failed_100hz"),
            stats_dir  = os.getenv("STATS_DIR_100HZ",  "/app/files/stats_100hz"),
            finished_dir=os.getenv("FINISHED_DIR_100HZ", "/app/files/finished_100hz"),
            timestamp_re=TIMESTAMP_RE,
            datetime_fmt="%Y-%m-%d %H-%M-%S",
            redis_db    =redis_db,
        ),
    ]

    for p in pipelines:
        logger.info(f"Started pipeline {p.name} watching {p.input}.")
    threading.Event().wait()

if __name__=="__main__":
    main()
