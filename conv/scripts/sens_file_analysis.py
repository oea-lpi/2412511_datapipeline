import logging
from pathlib import Path
from typing import Dict, Tuple, Optional

import pandas as pd
import redis

from helper.processing import move_to_finished

logger = logging.getLogger(__name__)


def check_readability(file_path: Path) -> bool:
    """
    Inital check to validate that path is a file and a .parquet/.csv file.

    Args:
        file_path: Path object to the currently to be processed file. 

    Returns:
        bool: True if ok, False if not ok.
    """
    if not file_path.is_file():
        logger.error(f"File not found: {file_path}.")
        return False
    if file_path.suffix.lower() not in {".parquet", ".csv"}:
        logger.error(f"Unsupported filetype (need .parquet or .csv): {file_path}")
        return False
    return True


def read_table(file_path: Path) -> pd.DataFrame:
    ext = file_path.suffix.lower()
    if ext == ".parquet":
        return pd.read_parquet(file_path)
    if ext == ".csv":
        return pd.read_csv(file_path)
    raise ValueError(f"Unsupported extension: {ext}")


def _row_to_redis_mapping(filename: str, row: pd.Series, timestamp: Optional[pd.Timestamp]) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    # include timestamp as its own field if we have it (not a column in Parquet)
    if timestamp is not None:
        mapping[f"{filename}_timestamp"] = timestamp.isoformat()

    for col, val in row.items():
        if pd.isna(val):
            sval = ""
        elif isinstance(val, pd.Timestamp):
            sval = val.isoformat()
        else:
            sval = str(val)
        mapping[f"{filename}_{col}"] = sval
    return mapping


def file_analysis(file_path: Path) -> Tuple[str, Dict[str, str]]:
    df = read_table(file_path)
    if df.empty:
        raise ValueError(f"File has no rows: {file_path}")

    filename = file_path.stem
    redis_key = f"stats:{filename}"

    if isinstance(df.index, pd.DatetimeIndex):
        # ensure chronological order, then pick last (newest) row
        sdf = df.sort_index(ascending=True, kind="mergesort")
        latest_ts: pd.Timestamp = sdf.index[-1]
        row = sdf.iloc[-1]
        mapping = _row_to_redis_mapping(filename, row, latest_ts)
        return redis_key, mapping

    # fallbacks if it's NOT a DatetimeIndex (e.g., some CSVs):
    # 1) Try to parse the first column as timestamps and select max.
    try:
        ts = pd.to_datetime(df.iloc[:, 0], errors="coerce", utc=True)
        if not ts.isna().all():
            newest_idx = ts.idxmax()
            row = df.loc[newest_idx]
            latest_ts = ts[newest_idx]
            mapping = _row_to_redis_mapping(filename, row, latest_ts)
            return redis_key, mapping
    except Exception:
        pass

    # 2) Final fallback: just take the last row as is (no timestamp available).
    row = df.iloc[-1]
    mapping = _row_to_redis_mapping(filename, row, timestamp=None)
    return redis_key, mapping


def redis_push(redis_db: redis.Redis, redis_key: str, mapping: Dict[str, str], TTL: int = 60) -> None:
    if not mapping:
        raise ValueError("Empty mapping, nothing to push.")
    
    pipe = redis_db.pipeline(transaction=True)
    pipe.hset(redis_key, mapping=mapping)
    pipe.expire(redis_key, TTL)
    pipe.execute()
    logger.info(f"Pushed {len(mapping)} fields to Redis key '{redis_key}'.")


def main(file_path: Path, finished_dir: Path, redis_db: redis.Redis):
    if not check_readability(file_path):
        raise RuntimeError(f"Readability check failed for {file_path}")

    redis_key, mapping = file_analysis(file_path)
    redis_push(redis_db, redis_key, mapping)
    move_to_finished(file_path, finished_dir)
