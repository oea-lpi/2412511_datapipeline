from datetime import datetime
from io import StringIO
import logging
from pathlib import Path
import re

import pandas as pd
import redis

from helper.processing import move_to_finished

logger = logging.getLogger(__name__)

def check_readability(file_path: Path) -> None:
    """
    Simple check for file existence and filetype being .csv.

    Args:
        file_path: Path object to the currently to be processed file. 
    """
    if not file_path.is_file():
        logger.error(f"File not found: {file_path}.")
        return
    
    if file_path.suffix.lower() != ".csv":
        logger.error(f"Called on non-.csv file: {file_path}.")
        return

def file_analysis(file_path: Path, finished_dir: Path) -> None:
    """
    Main processing flow for recognized CSV's from Sensical
    Current: Read files, write data to redis, move the file to finished dir. Failed files are moved on Pipeline level.

    Args:
        file_path: Path object to the currently to be processed file. 
    """

    lines = file_path.read_text(encoding="utf-8").splitlines()
    
    # --- Helper ---
    def find_idx(pattern):
        rx = re.compile(pattern, re.IGNORECASE)
        for i, ln in enumerate(lines):
            if rx.search(ln):
                return i
        return None
    
    meta = {}

    # Title (e.g., "Bauwerk R6-07 - Sensor Nord")
    meta["title"] = lines[0].strip()

    i_time = find_idx(r"^\s*Zeit\s")
    if i_time is not None:
        ts_raw = lines[i_time].split("Zeit", 1)[1].strip()
        # Example format: 22-Apr-2025 12:26:43  (day-month_abbr-year)
        meta["timestamp"] = pd.to_datetime(ts_raw, format="%d-%b-%Y %H:%M:%S", dayfirst=True)
    
    # Quantiles ('q50 q90 max wCr')
    i_qhdr = find_idx(r"^\s*q50\s+q90\s+max\s+wCr\s*$")
    if i_qhdr is not None and i_qhdr + 1 < len(lines):
        qvals = re.split(r"\s+", lines[i_qhdr + 1].strip())
        qvals = [v.replace(",", ".") for v in qvals if v]
        q50, q90, wcr_max = map(float, qvals[:3])
        meta["q50_m^^^m"] = q50
        meta["q90_mm"] = q90
        meta["wCr_max_mm"] = wcr_max

    # Number of cracks
    i_count = find_idx(r"Anzahl\s+erkannter\s+Risse")
    if i_count is not None:
        m = re.search(r"(\d+)", lines[i_count])
        if m:
            meta["crack_count"] = int(m.group(1))
    
    # Data block
    i_block = find_idx(r"Rissposition\s*\(.*\)\s*vs\.")
    if i_block is None:
        raise ValueError("Could not find data block header.")
    
    # Expect header on next line
    hdr_line = lines[i_block + 1].strip()
    headers = re.split(r"\s+", hdr_line)
    # Standardize expected names: X, Y, Z, wCr
    expected = ["X", "Y", "Z", "wCr"]
    if len(headers) >= 4:
        headers = expected
    else:
        headers = expected
    
    # Collect data lines until "End"
    data_lines = []
    for ln in lines[i_block + 2:]:
        if ln.strip().lower().startswith("end"):
            break
        if not ln.strip():
            continue
        # Keep only lines that look like 4 numbers
        nums = re.findall(r"[-+]?\d+(?:[.,]\d+)?", ln)
        if len(nums) >= 4:
            nums = [n.replace(",", ".") for n in nums[:4]]
            data_lines.append(" ".join(nums))
    
    if not data_lines:
        raise ValueError("No data rows found in report.")
    
    df = pd.read_csv(StringIO("\n".join(data_lines)),
                     sep=r"\s+", header=None, names=headers, engine="python")
    
    for col in ["X", "Y", "Z", "wCr"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    
    df.attrs["units"] = {"X": "m", "Y": "m", "Z": "m", "wCr": "mm"}
    return meta, df

def main():
    pass
