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

from config.setup_logging import setup_logging
from helper.utility import extract_ts
from scripts.udbf_file_analysis import udbf_file_analysis
from scripts.watcher import Watcher


logger = logging.getLogger("conv_lpi")

TIMESTAMP_RE = re.compile(r'(\d{4}-\d{2}-\d{2})_(\d{2}-\d{2}-\d{2})')

def start_heartbeat(redis_client, key="heartbeat_converter", interval=20, ttl=60):
    """
    Every 'interval' seconds write key with 'ttl" expiry.
    """
    def loop():
        while True:
            try:
                redis_client.set(key, "0", ex=ttl)
            except Exception:
                logger.exception("Failed to write heartbeat for converter.")
            finally:
                time.sleep(interval)

    t = threading.Thread(target=loop, daemon=True)
    t.start()

finished_counter_redis = None

class Pipeline:
    def __init__(self, name: str, input_dir: str, failed_dir: str, stats_dir: str, finished_dir: str, timestamp_re: re.Pattern[str], datetime_fmt: str):
        self.name = name
        self.input = Path(input_dir)
        self.failed = Path(failed_dir)
        self.stats = Path(stats_dir)
        self.finished = Path(finished_dir)
        self.timestamp_re = timestamp_re
        self.datetime_fmt = datetime_fmt
        self.queue = Queue()
        self.lock  = threading.Lock()
        self.processed = set()

        # Start worker
        t = threading.Thread(target=self.worker, daemon=True)
        t.start()

        # Start watcher
        obs = PollingObserver()
        handler = Watcher(self.enqueue, self.schedule_next, str(self.input))
        obs.schedule(handler, self.input, recursive=False)
        obs.start()

        # Enqueue all-but-newest at startup
        all_dat = [p for p in self.input.iterdir() if p.suffix == ".dat"]
        for p in sorted(all_dat, key=self._ts())[:-1]:
            self.enqueue(p)

    def _ts(self, path: Path) -> datetime:
        return extract_ts(path, self.timestamp_re, self.datetime_fmt)

    def enqueue(self, path: Path | str) -> None:
        p = Path(path)
        with self.lock:
            if p not in self.processed:
                self.processed.add(p)
                self.queue.put(p)

    def schedule_next(self, _) -> None:
        # If >=2 files remain, pick the oldest
        dats = [p for p in self.input.iterdir() if p.suffix == ".dat"]
        if len(dats) > 1:
            self.enqueue(min(dats, key=self._ts()))

    def worker(self) -> None:
        global finished_counter_redis
        while True:
            p: Path = self.queue.get()
            try:
                logger.info(f"[{self.name}] processing {p}")

                # Main analysis loop
                udbf_file_analysis(
                    str(p),
                    failed_dir=str(self.failed),
                    stats_dir=str(self.stats),
                    finished_dir=str(self.finished),
                )

                # Count done filecount
                count = sum(1 for f in self.finished.iterdir() if f.suffix == ".dat")
                # Key like "converter:num_finished_10hz" or "converter:num_finished_100hz"
                key = f"converter:num_finished_{self.name.lower()}"
                finished_counter_redis.set(key, count)
                logger.debug(f"Pushed finished count: {key} = {count}.")
            except Exception:
                hb_key = os.getenv("HEARTBEAT_KEY_CONVERTER", "heartbeat_converter")
                hb_ttl = int(os.getenv("HEARTBEAT_TTL", 60))
                finished_counter_redis.set(hb_key, 1, ex=hb_ttl)

                logger.exception(f"[{self.name}] failed on {p}, moving to failed dir.")
                dest = self.failed / p.name
                try:
                    shutil.move(str(p), str(dest))
                    logger.info(f"Moved bad file to {dest}.")
                except Exception:
                    logger.exception(f"Could not move {p} to failed dir.")
            finally:
                self.queue.task_done()
                # Immediately look for the next one
                self.schedule_next(None)

def main():
    setup_logging()
    global finished_counter_redis

    # Build a shared redis client for heartbeats.
    hb_redis = redis.Redis(
        host=os.getenv("REDIS_HOST","redis"),
        port=int(os.getenv("REDIS_PORT",6379)),
        db=int(os.getenv("REDIS_DB",0)),
        decode_responses=True
    )

    finished_counter_redis = hb_redis

    # Start heartbeat thread before pipelines come up.
    start_heartbeat(
      redis_client=hb_redis,
      key=os.getenv("HEARTBEAT_KEY_CONVERTER","heartbeat_converter"),
      interval=int(os.getenv("HEARTBEAT_INTERVAL",20)),
      ttl=int(os.getenv("HEARTBEAT_TTL",60))
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
            datetime_fmt="%Y-%m-%d %H-%M-%S"
        ),
        Pipeline(
            name       = "100Hz",
            input_dir  = os.getenv("INPUT_DIR_100HZ",  "/app/files/input_100hz"),
            failed_dir = os.getenv("FAILED_DIR_100HZ", "/app/files/failed_100hz"),
            stats_dir  = os.getenv("STATS_DIR_100HZ",  "/app/files/stats_100hz"),
            finished_dir=os.getenv("FINISHED_DIR_100HZ", "/app/files/finished_100hz"),
            timestamp_re=TIMESTAMP_RE,
            datetime_fmt="%Y-%m-%d %H-%M-%S"
        ),
    ]

    for p in pipelines:
        logger.debug(f"Started pipeline {p.name} watching {p.input}.")
    threading.Event().wait()

if __name__=="__main__":
    main()
