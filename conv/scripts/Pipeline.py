from dataclasses import dataclass
from datetime import datetime
import logging
import os
from pathlib import Path
from queue import Queue
import re
import shutil
import threading
from typing import Optional
import time

import redis
from watchdog.observers.polling import PollingObserver

from helper.utility import extract_ts
from .watcher import Watcher


logger = logging.getLogger(__name__)

BASIC_REDIS_TTL = int(os.getenv("BASIC_REDIS_TTL", "60"))
CONV_CONTEXT = os.getenv("CONV_CONTEXT")
STABLE_CHECKS = int(os.getenv("STABLE_CHECKS", "2")) # consecutive identical stat() results   
MIN_FILE_AGE_SEC = float(os.getenv("MIN_FILE_AGE_SEC", "40.0")) # min seconds since last mtime
TICKER_INTERVAL_SEC = float(os.getenv("TICKER_INTERVAL_SEC", "2.0"))  # periodic rescan

@dataclass
class _StatInfo:
    size: int
    mtime: float
    stable_count: int

class Pipeline:
    """
    Monitors a specified folder and starts the processing pipeline.
    Files are enqueued only when they are considered 'stable' (size & mtime
    unchanged for STABLE_CHECKS polls and older than MIN_FILE_AGE_SEC).
    """
    def __init__(self, 
        name: str, 
        input_dir: str, 
        failed_dir: str, 
        finished_dir: str, 
        timestamp_re: re.Pattern[str], 
        datetime_fmt: str, 
        redis_db: redis.Redis,
        stats_dir: Optional[str] = None, 
    ):
        self.name = name
        self.input = Path(input_dir)
        self.failed = Path(failed_dir)
        if stats_dir:
            self.stats = Path(stats_dir)
        self.finished = Path(finished_dir)
        self.timestamp_re = timestamp_re
        self.datetime_fmt = datetime_fmt
        self.redis_db = redis_db

        self.queue: Queue[Path] = Queue()
        self.lock = threading.Lock()
        self.processed: set[Path] = set() 
        self._seen: dict[Path, _StatInfo] = {}  

        t = threading.Thread(target=self.worker, daemon=True, name=f"worker:{self.name}")
        t.start()

        observer = PollingObserver()
        handler = Watcher(self.enqueue, self.schedule_next, str(self.input))
        observer.schedule(handler, self.input, recursive=False)
        observer.start()
        self.observer = observer
        self.handler = handler  

        threading.Thread(target=self._ticker, daemon=True, name=f"ticker:{self.name}").start()
        self.schedule_next(None)

    def _ticker(self) -> None:
        while True:
            time.sleep(TICKER_INTERVAL_SEC)
            try:
                self.schedule_next(None)
            except Exception:
                logger.exception(f"Ticker scan failed: {self.name}")

    def _stat(self, p: Path) -> os.stat_result | None:
        try:
            return p.stat()
        except FileNotFoundError:
            self._seen.pop(p, None)
            return None
        except Exception:
            logger.exception(f"{self.name} stat() failed for {p}")
            return None

    def _is_stable(self, p: Path) -> bool:
        st = self._stat(p)
        if not st:
            return False

        now = time.time()
        # must be older than MIN_FILE_AGE_SEC
        if (now - st.st_mtime) < MIN_FILE_AGE_SEC:
            # reset the seen info (only consecutive identical stats)
            prev = self._seen.get(p)
            if prev is None or prev.size != st.st_size or prev.mtime != st.st_mtime:
                self._seen[p] = _StatInfo(size=st.st_size, mtime=st.st_mtime, stable_count=1)
            else:
                prev.stable_count += 1
            return False

        prev = self._seen.get(p)
        if prev and prev.size == st.st_size and prev.mtime == st.st_mtime:
            prev.stable_count += 1
        else:
            self._seen[p] = _StatInfo(size=st.st_size, mtime=st.st_mtime, stable_count=1)

        info = self._seen[p]
        return info.stable_count >= STABLE_CHECKS

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
        """
        Scan the input dir, find the oldest *stable* file not processed and enqueue it. 
        Runs on FS events and a periodic ticker.
        """
        try:
            dats = [p for p in self.input.iterdir() if p.is_file()]
        except FileNotFoundError:
            return
        
        candidates: list[tuple[datetime, Path]] = []
        for p in dats:
            if p in self.processed:
                continue

            ts = self._ts(p)
            if ts is None:
                continue

            if self._is_stable(p):
                candidates.append((ts, p))
        if not candidates:
            return

        _, oldest = min(candidates)  # oldest timestamp first
        self.enqueue(oldest)

    def worker(self) -> None:
        while True:
            file_path: Path = self.queue.get()
            remove_from_processed = False  # don't requeue infinitely if move fails
            try:
                logger.info(f"[{self.name}] processing {file_path}")
                if CONV_CONTEXT == "LPI":
                    from .udbf_file_analysis import udbf_file_analysis
                    udbf_file_analysis(
                        file_path = file_path,
                        stats_dir = self.stats,
                        finished_dir = self.finished,
                        redis_db = self.redis_db
                    )
                elif CONV_CONTEXT == "SENS":
                    from .sens_file_analysis import main as sens_file_analysis
                    sens_file_analysis(
                        file_path = file_path,
                        finished_dir = self.finished,
                        redis_db = self.redis_db
                    )
                elif CONV_CONTEXT == "MIST":
                    from .mist_file_analysis import main as mist_file_analysis
                    mist_file_analysis(
                        file_path = file_path,
                        finished_dir = self.finished,
                        redis_db = self.redis_db
                    )
                else:
                    logger.error("Unknown CONV_CONTEXT=%r for %s", self.name, CONV_CONTEXT, file_path)

                remove_from_processed = True
                self.redis_db.set(f"health:{self.name}_file_processing", 0, ex=BASIC_REDIS_TTL) 
            except Exception:
                logger.exception(f"[{self.name}] failed on {file_path}, moving to failed dir.")
                dest = self.failed / file_path.name
                try:
                    shutil.move(str(file_path), str(dest))
                    logger.info(f"Moved bad file to {dest}.")
                    self.redis_db.set(f"health:{self.name}_file_processing", 1, ex=BASIC_REDIS_TTL) 
                    remove_from_processed = True
                except Exception:
                    logger.exception(f"Could not move {file_path} to failed dir.")
            finally:
                with self.lock:
                    if remove_from_processed:
                        self.processed.discard(file_path)

                self.queue.task_done()
                try:
                    self.schedule_next(None)
                except Exception:
                    logger.exception(f"schedule_next failed at worker tail: {self.name}")
    
    def archiver():
        #TODO Placeholder for function to move files from finished into finished_archive, maybe relevant if file number in folder gets to big 
        ...

    def stop(self) -> None:
        #Graceful shutdown for testing purpose.
        try:
            self.observer.stop()
            self.observer.join(timeout=5)
        except Exception:
            logger.exception("Failed to stop observer.")