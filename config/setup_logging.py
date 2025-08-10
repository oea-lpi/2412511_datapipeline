import json
import logging.config
from pathlib import Path

def setup_logging(process_name: str) -> None:
    """
    Set up Python logging using a JSON configuration file.

    Args:
        process_name (str): A name representing the current process (e.g., 'importer', 'worker').

    Returns:
        None
    """
    with open("config/logger_config.json") as f:
        config = json.load(f)
        
    log_path = Path(config["handlers"]["file"].get("filename", "logs/app.log.jsonl"))
    if "." in log_path.name:
        root, rest = log_path.name.split(".", 1)
    else:
        root, rest = log_path.name, ""
    new_name = f"{root}_{process_name}"
    if rest:
        new_name += f".{rest}"

    new_log_path = log_path.parent / new_name    
    config["handlers"]["file"]["filename"] = str(new_log_path)

    logging.config.dictConfig(config)