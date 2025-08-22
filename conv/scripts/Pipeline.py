from datetime import datetime
import logging
import os
from pathlib import Path
from queue import Queue
import re
import shutil
import threading

import redis
from watchdog.observers.polling import PollingObserver

from helper.utility import extract_ts
from .udbf_file_analysis import udbf_file_analysis
from .sens_file_analysis import main as sens_file_analysis
from .watcher import Watcher


logger = logging.getLogger(__name__)

BASIC_REDIS_TTL = int(os.getenv("BASIC_REDIS_TTL", "60"))
CONV_CONTEXT = os.getenv("CONV_CONTEXT")

class Pipeline:
    def __init__(self, 
        name: str, 
        input_dir: str, 
        failed_dir: str, 
        stats_dir: str, 
        finished_dir: str, 
        timestamp_re: re.Pattern[str], 
        datetime_fmt: str, 
        redis_db: redis.Redis
    ):
        self.name = name
        self.input = Path(input_dir)
        self.failed = Path(failed_dir)
        self.stats = Path(stats_dir)
        self.finished = Path(finished_dir)
        self.timestamp_re = timestamp_re
        self.datetime_fmt = datetime_fmt
        self.redis_db = redis_db

        self.queue = Queue()
        self.lock  = threading.Lock()
        self.processed = set()

        # Start worker
        t = threading.Thread(target=self.worker, daemon=True, name=f"worker:{self.name}")
        t.start()

        # Start watcher
        observer = PollingObserver()
        handler = Watcher(self.enqueue, self.schedule_next, str(self.input))
        observer.schedule(handler, self.input, recursive=False)
        observer.start()

        # Enqueue all-but-newest files at startup
        all_dat = [p for p in self.input.iterdir()]
        for p in sorted(all_dat, key=self._ts)[:-1]: 
            self.enqueue(p)

    def _ts(self, path: Path) -> datetime | None:
        try:
            return extract_ts(path, self.timestamp_re, self.datetime_fmt)
        except:
            logger.warning("Skipping file with unparsable timestamp: %s", path)
            return None

    def enqueue(self, path: Path | str) -> None:
        p = Path(path)
        with self.lock:
            if p not in self.processed:
                self.processed.add(p)
                self.queue.put(p)

    def schedule_next(self, _) -> None:
        dats = [p for p in self.input.iterdir() if p.is_file()]
        candidates: list[tuple[datetime, Path]] = []
        for p in dats:
            if p in self.processed:
                continue
            ts = self._ts(p)       
            if ts is not None:
                candidates.append((ts, p)) 
        if len(candidates) > 1:
            _, oldest = min(candidates) 
            self.enqueue(oldest)

    def worker(self) -> None:
        while True:
            file_path: Path = self.queue.get()
            remove_from_processed = False #Flag so that faulty files that could not be moved do not get infinitely requeued
            try:
                logger.info(f"[{self.name}] processing {file_path}")
                if CONV_CONTEXT == "LPI":
                    udbf_file_analysis(
                        file_path = file_path,
                        stats_dir = self.stats,
                        finished_dir = self.finished,
                        redis_db = self.redis_db
                    )
                elif CONV_CONTEXT == "SENS":
                    sens_file_analysis(
                        file_path = file_path,
                        finished_dir = self.finished,
                        redis_db = self.redis_db
                    )
                elif CONV_CONTEXT == "MIST":
                    sens_file_analysis(
                        file_path = file_path,
                        finished_dir = self.finished,
                        redis_db = self.redis_db
                    )
                remove_from_processed = True 
            except Exception:
                logger.exception(f"[{self.name}] failed on {file_path}, moving to failed dir.")
                dest = self.failed / file_path.name
                try:
                    shutil.move(str(file_path), str(dest))
                    logger.info(f"Moved bad file to {dest}.")
                    self.redis_db.set(f"health:{self.name}_file_processing", 1, ex=BASIC_REDIS_TTL)
                except Exception:
                    logger.exception(f"Could not move {file_path} to failed dir.")
            finally:
                with self.lock:
                    if remove_from_processed:
                        self.processed.discard(file_path)

                self.queue.task_done()
                self.schedule_next(None)
    
    def stop(self) -> None:
        #Graceful shutdown for testing purpose.
        try:
            self.observer.stop()
            self.observer.join(timeout=5)
        except Exception:
            logger.exception("Failed to stop observer.")