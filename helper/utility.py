from datetime import datetime
from pathlib import Path
import re

def extract_ts(path: Path, timestamp_re: re.Pattern[str], fmt: str) -> datetime:
    """
    Parse the timestamp from file and output the datetime in isoformat.

    Args:
    path (Path): Path to the input file.
    timestamp_re (re.Pattern[str]): Compiled regex pattern to extract timestamp.
    fmt (str): Datetime format string to parse the matched timestamp.

    Returns:
        datetime: The extracted or fallback timestamp as a datetime object.
    """
    match = timestamp_re.search(path.name)
    if match:
        date_part = match.group(1)
        time_part = match.group(2)
        
        datetime_str = f"{date_part} {time_part}"
        return datetime.strptime(datetime_str, fmt).isoformat()
    return datetime.fromtimestamp(path.stat().st_mtime)