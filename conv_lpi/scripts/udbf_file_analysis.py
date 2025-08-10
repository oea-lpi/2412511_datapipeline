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

from data_operations.DataConverterUDBF import DataConverterUDBF

logger = logging.getLogger(__name__)

# Config
ALARM_DIR = Path(os.getenv("ALARMED_DIR", "/app/files/alarmed"))
TIMEZONE = os.getenv("TIMEZONE", "Europe/Berlin")
INTERNAL_ALLSAT_FINISHED = Path(os.getenv("INTERNAL_ALLSAT_FINISHED", "/app/files/allsat/finished"))

# Redis setup
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB   = int(os.getenv("REDIS_DB", 0))
REDIS_STATS_TTL = int(os.getenv("REDIS_STATS_TTL", 60))

# Patterns
ALLSAT_PATTERN = re.compile(r'FHEB_(\d{4}_\d{2}_\d{2}_\d{2}_\d{2}_\d{2})')

redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    db=  REDIS_DB
)

def udbf_file_analysis(file_path: str, failed_dir: str, stats_dir: str, finished_dir: str) -> None:
    """
    Main work loop for recognized .dat files.
    """

    # Sanity checks
    if not os.path.isfile(file_path):
        logger.error(f"File not found: {file_path}")
        return
    
    if not file_path.lower().endswith('.dat'):
        logger.error(f"Called on non-.dat file: {file_path}")
        return
    
    round_factor = 3
    raw_file = os.path.basename(file_path)
    path_dir = os.path.dirname(file_path)

    conv = DataConverterUDBF(
        raw_file,              # original filename
        path_dir,              # input directory
        file_path,             # full path
        round_factor
    )

    conv.check_readability_of_data_file()
    conv.read_udbf_file()
    conv.date_converter()
    conv.save_statistics_csv(stats_dir)


    # Alarmed logic
    if Path(stats_dir).stem.endswith("stats"):
        base = Path(file_path).stem 

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
                stats100hz     = Path(os.getenv("STATS_DIR_100HZ",    "/app/files/stats_100hz"))
                dat100 = finished_100hz / f"{base}.dat"
                csv100 = stats100hz    / f"{base}_stats.csv"

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
                    # Fallback to mtime if name isn’t in the expected format
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

                break

    """
    # Alarmed logic
    if os.path.basename(stats_dir).endswith("stats"):
        base = os.path.splitext(os.path.basename(file_path))[0]

        for idx, name in enumerate(conv.channel_names):
            if (name.endswith("_GAL") or name.endswith("_RAL")) and conv.data[:,idx].max() == 1:
                target = os.path.join(ALARM_DIR, base)
                os.makedirs(target, exist_ok=True)

                # Copy 1Hz .dat file and .csv
                shutil.copy(file_path, os.path.join(target, os.path.basename(file_path)))
                stats1 = os.path.join(stats_dir, base + "_stats.csv")
                if os.path.exists(stats1):
                    shutil.copy(stats1, os.path.join(target, os.path.basename(stats1)))

                # Copy 100Hz .dat file and .csv
                finished_100hz = os.getenv("FINISHED_DIR_100HZ", "/app/files/finished_100hz")
                stats100hz = os.getenv("STATS_DIR_100HZ", "/app/files/stats_100hz")
                dat100 = os.path.join(finished_100hz, base + ".dat")
                csv100 = os.path.join(stats100hz, base + "_stats.csv")

                # Give the 100Hz pipeline up to 30 s to finish
                wait = 0
                while wait < 30 and not os.path.exists(dat100):
                    time.sleep(1); wait += 1
                if os.path.exists(dat100):
                    shutil.copy(dat100, os.path.join(target, os.path.basename(dat100)))

                # and wait a bit more for the 100 Hz stats.csv
                while wait < 180 and not os.path.exists(csv100):
                    time.sleep(2); wait += 2
                if os.path.exists(csv100):
                    shutil.copy(csv100, os.path.join(target, os.path.basename(csv100)))
                break

    # "Alarm" logic for 100hz files.
    # If any channel endswith _GAL or _RAL and its max==1,
    # copy .dat + stats.csv into alarm folder.
    alarm_dir = os.getenv("ALARMED_100HZ_DIR", "/app/files/alarmed_100hz")
    for idx, name in enumerate(conv.channel_names):
        if (name.endswith("_GAL") or name.endswith("_RAL")) and conv.data[:, idx].max() == 1:
            # build a subfolder named like the dat file (sans “.dat”)
            base = os.path.splitext(os.path.basename(file_path))[0]
            target = os.path.join(alarm_dir, base)
            os.makedirs(target, exist_ok=True)
            # copy raw .dat
            shutil.copy(file_path, os.path.join(target, os.path.basename(file_path)))
            # copy its stats CSV
            stats_csv = os.path.join(stats_dir, base + "_stats.csv")
            if os.path.exists(stats_csv):
                shutil.copy(stats_csv, os.path.join(target, os.path.basename(stats_csv)))
            break"""

    # Publish stats to redis hash
    key = f"stats:{raw_file.replace('.dat','')}"
    mapping: dict[str,str] = {}
    try:
        for _, row in conv.df_stats.iterrows():
            sensor = row["Sensor"]
            mapping.update({
                f"{sensor}:last"   : row["Last Value"],
                f"{sensor}:mean"   : row["Mean"],
                f"{sensor}:median" : row["Median"],
                f"{sensor}:min"    : row["Minimum"],
                f"{sensor}:max"    : row["Maximum"]
            })

        if mapping:
            pipe = redis_client.pipeline()
            pipe.hset(key, mapping=mapping)
            pipe.expire(key, REDIS_STATS_TTL)
            pipe.execute()
        else:
            logger.warning(f"No stats to publish for {raw_file!r}, skipping.")

        logger.info(f"Published {len(mapping)} fields to {key!r}, TTL={REDIS_STATS_TTL}s")
    except Exception:
        logger.exception(f"Failed to push stats to Redis for {raw_file}")

    conv.move_to_finished(finished_dir)


