from datetime import datetime
import logging
from pathlib import Path
import re

logger = logging.getLogger(__name__)

def extract_ts(path: Path, timestamp_re: re.Pattern[str], fmt: str) -> datetime:
    """
    Parse the timestamp from filename and output the datetime in isoformat.

    Args:
        path: Path to the input file.
        timestamp_re: Compiled regex pattern to extract timestamp.
        fmt: Datetime format string to parse the matched timestamp.

    Returns:
        datetime: The extracted or fallback timestamp (last modification date) as a datetime object.
    """
    match = timestamp_re.search(path.name)
    if match:
        date_part = match.group(1)
        time_part = match.group(2)
        
        datetime_str = f"{date_part} {time_part}"
        return datetime.strptime(datetime_str, fmt)
    return datetime.fromtimestamp(path.stat().st_mtime)
