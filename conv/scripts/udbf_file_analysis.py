import csv
from datetime import datetime, timezone, timedelta
import logging
import os
from pathlib import Path
import re
import shutil
import time
from zoneinfo import ZoneInfo

import redis

from gantner_operations.DataConverterUDBF import DataConverterUDBF


logger = logging.getLogger(__name__)

BASIC_REDIS_TTL = int(os.getenv("BASIC_REDIS_TTL", "60"))
BASIC_ROUNDING = int(os.getenv("BASIC_ROUNDING", "3"))

HEALTH_LPI_100HZ_FILE_SIZE = os.getenv("HEALTH_LPI_100HZ_FILE_SIZE", "health:lpi_100hz_file_size")
HEALTH_LPI_1HZ_FILE_SIZE = os.getenv("HEALTH_LPI_1HZ_FILE_SIZE", "health:lpi_1hz_file_size")

def udbf_file_analysis(file_path: Path, stats_dir: Path, finished_dir: Path, redis_db: redis.Redis) -> None:
    """
    Main processing flow for recognized DAT files.
    Current: Read files, create a CSV with statistical values, write data to redis, move the file to finished dir. Failed files are moved on Pipeline level.

    Args:
        file_path: Path object to the currently to be processed file.
        failed_dir: General path to the directory for failed files.
        stats_dir: General path to the directory statistics files.
        finished_dir: General path to the directory processed files.    
        redis_db: Redis databank to save values to.    
    """

    # Sanity checks
    if not file_path.is_file():
        logger.error(f"File not found: {file_path}")
        return
    
    if file_path.suffix.lower() != ".dat":
        logger.error(f"Called on non-.dat file: {file_path}")
        return
    
    raw_file = file_path.name
    path_dir = file_path.parent

    conv = DataConverterUDBF(
        str(raw_file),              # original filename
        str(path_dir),              # input directory
        str(file_path),             # full path
        BASIC_ROUNDING
    )

    health_file_size = conv.check_filesize()
    if "100hz" in raw_file.lower():
        redis_db.set(HEALTH_LPI_100HZ_FILE_SIZE, health_file_size, ex=BASIC_REDIS_TTL)
    elif "1hz" in raw_file.lower():
        redis_db.set(HEALTH_LPI_1HZ_FILE_SIZE, health_file_size, ex=BASIC_REDIS_TTL)
    else:
        pass

    conv.read_udbf_file()
    conv.date_converter()
    conv.save_statistics_csv(str(stats_dir))
    
    # Publish stats to redis hash
    key = f"stats:{raw_file.replace('.dat','')}"
    mapping: dict[str,str] = {}
    try:
        for _, row in conv.df_stats.iterrows():
            sensor = row["Sensor"]
            mapping.update({
                f"{sensor}:mean"   : row["Mean"],
                f"{sensor}:min"    : row["Minimum"],
                f"{sensor}:max"    : row["Maximum"]
            })
        if mapping:
            pipe = redis_db.pipeline()
            pipe.hset(key, mapping=mapping)
            pipe.expire(key, BASIC_REDIS_TTL)
            pipe.execute()
        else:
            logger.warning(f"No stats to publish for {raw_file!r}, skipping.")

        logger.debug(f"Published {len(mapping)} fields to {key!r}, TTL={BASIC_REDIS_TTL}s")
    except Exception:
        logger.exception(f"Failed to push stats to Redis for {raw_file}")

    conv.move_to_finished(str(finished_dir))


    """ OLD CODE SNIPPET FOR ALARMED LOGIC, TO SPECIFIC TO BE FACOTORIZED, ADJUST AS NEEDED    
    # Alarmed logic
    if stats_dir.stem.endswith("stats"):
        base = file_path.stem 

        for idx, name in enumerate(conv.channel_names):
            if (name.endswith("_GAL") or name.endswith("_RAL")) and conv.data[:, idx].max() == 1:
                target = ALARM_DIR / base
                target.mkdir(parents=True, exist_ok=True)

                # Copy 1Hz .dat file + its stats CSV
                src_dat1 = Path(file_path)
                src_csv1 = Path(stats_dir) / f"{base}_stats.csv"
                shutil.copy(src_dat1,  target / src_dat1.name)
                if src_csv1.exists():
                    shutil.copy(src_csv1, target / src_csv1.name)

                # Locate the matching 100 Hz paths
                finished_100hz = Path(os.getenv("FINISHED_DIR_100HZ", "/app/files/finished_100hz"))
                stats100hz = Path(os.getenv("STATS_DIR_100HZ", "/app/files/stats_100hz"))
                dat100 = finished_100hz / f"{base}.dat"
                csv100 = stats100hz / f"{base}_stats.csv"

                # Wait up to 30 s for the .dat to arrive
                waited = 0
                while waited < 30 and not dat100.exists():
                    time.sleep(1)
                    waited += 1
                if dat100.exists():
                    shutil.copy(dat100, target / dat100.name)

                # Then wait up to 180 s for the stats .csv
                while waited < 240 and not csv100.exists():
                    time.sleep(2)
                    waited += 2
                if csv100.exists():
                    shutil.copy(csv100, target / csv100.name)

                # Compute & copy the Allsat file
                # Allsat Timestamp is in German Timezone, .dat file is in UTC
                # Allsat Timestamp indicates end of measurement, .dat file timestamp indicates start of measurement
                m = ALLSAT_PATTERN.search(base)
                if m:
                    ts_utc = datetime.strptime(m.group(1), "%Y-%m-%d_%H-%M-%S").replace(tzinfo=timezone.utc)
                else:
                    # Fallback to mtime if name isn't in the expected format
                    ts_utc = datetime.fromtimestamp(Path(file_path).stat().st_mtime, tz=timezone.utc)
                ts_end_utc = ts_utc + timedelta(minutes=10)
                ts_berlin = ts_end_utc.astimezone(ZoneInfo(TIMEZONE))
                allsat_name = f"FHEB_{ts_berlin:%Y_%m_%d_%H_%M_%S}.csv"
                allsat_path = INTERNAL_ALLSAT_FINISHED / allsat_name

                waited = 0
                while waited < 240 and not allsat_path.exists():
                    time.sleep(2)
                    waited += 2

                if allsat_path.exists():
                    shutil.copy(allsat_path, target / allsat_path.name)

                break"""


