from datetime import datetime
import logging
from pathlib import Path
from queue import Queue
import shutil
import threading

from watchdog.observers.polling import PollingObserver

from helper.utility import extract_ts
from udbf_file_analysis import udbf_file_analysis
from watcher import Watcher

logger = logging.getLogger(__name__)

class Pipeline:
    def __init__(self, 
        name: str, 
        input_dir: str, 
        failed_dir: str, 
        stats_dir: str, 
        finished_dir: str, 
        timestamp_re: re.Pattern[str], 
        datetime_fmt: str, 
        redis_db: redis.Redis,
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

        # Enqueue all-but-newest .dat files at startup
        all_dat = [p for p in self.input.iterdir() if p.suffix == ".dat"]
        for p in sorted(all_dat, key=self._ts)[:-1]: 
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
        dats = [p for p in self.input.iterdir() if p.suffix.lower() == ".dat"]
        if len(dats) > 1:
            self.enqueue(min(dats, key=self._ts))

    def worker(self) -> None:
        while True:
            p: Path = self.queue.get()
            remove_from_processed = False #Flag so that faulty files that could not be moved do not get infinitely requeued
            try:
                logger.info(f"[{self.name}] processing {p}")
                udbf_file_analysis(
                    str(p),
                    failed_dir=str(self.failed),
                    stats_dir=str(self.stats),
                    finished_dir=str(self.finished),
                    redis_db=self.redis_db
                )
                remove_from_processed = True 
            except Exception:
                logger.exception(f"[{self.name}] failed on {p}, moving to failed dir.")
                dest = self.failed / p.name
                try:
                    shutil.move(str(p), str(dest))
                    logger.info(f"Moved bad file to {dest}.")
                except Exception:
                    logger.exception(f"Could not move {p} to failed dir.")
            finally:
                with self.lock:
                    if remove_from_processed:
                        self.processed.discard(p)

                self.queue.task_done()
                self.schedule_next(None)
    
    def stop(self) -> None:
        #Graceful shutdown for testing purpose.
        try:
            self.observer.stop()
            self.observer.join(timeout=5)
        except Exception:
            logger.exception("Failed to stop observer.")