import logging
from pathlib import Path
import shutil

logger = logging.getLogger(__name__)

def move_to_finished(file_path: Path, dest_path: Path) -> bool:
    """ 
    Move a sepcified file into a folder.

    Args:
        file_path: Path object containing full path to file.
        dest_path: Path object containing destination path.

    Returns:
        True: If file was successfully moved.
    
    Raises:
        FileNotFoundError: Source file could not be found.
        Exception: Failed to move file into destination path.
    """
    if not file_path.is_file():
        logger.warning(f"Source file not found: {file_path}")
        raise FileNotFoundError(file_path)
        
    dest_path = dest_path / file_path.name
    try:
        shutil.move(file_path, dest_path)
        logger.debug(f"Moved {file_path.name} to {dest_path}")
        return True
    except Exception as e:
        logger.warning(f"Failed to move {file_path.name} to {dest_path}: {e}")
        raise