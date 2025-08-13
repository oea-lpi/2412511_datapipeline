import logging
from typing import Callable

from watchdog.events import FileSystemEventHandler, FileMovedEvent, FileCreatedEvent, FileSystemEvent


logger = logging.getLogger(__name__)

class Watcher(FileSystemEventHandler):
    """
    Watchdog to react when a .dat file appears in the input folder.
    'schedule_next' looks at *all* files and enqueue only the oldest if >1 exist.
    """
    def __init__(self, enqueue_fn: Callable[[str], None], schedule_next_fn: Callable[[str], None], input_dir: str) -> None:
        super().__init__()
        self.enqueue = enqueue_fn
        self.schedule_next = schedule_next_fn
        self.input_dir = input_dir

    def on_created(self, event: FileSystemEvent) -> None:
        if isinstance(event, FileCreatedEvent) and not event.is_directory and event.src_path.lower().endswith('.dat'):
            logger.debug(f"Detected created: {event.src_path}")
            self.schedule_next(self.input_dir)

    def on_moved(self, event: FileSystemEvent) -> None:
        if isinstance(event, FileMovedEvent) and not event.is_directory and event.dest_path.lower().endswith('.dat'):
            logger.debug(f"Detected moved: {event.dest_path}")
            self.schedule_next(self.input_dir)

    def on_modified(self, event: FileSystemEvent) -> None:
        if not event.is_directory and event.src_path.lower().endswith('.dat'):
            # Commenting it out for spam reasons -> .dat files get constantly modified in the folder.
            # logger.debug(f"Detected modified: {event.src_path}")
            self.schedule_next(self.input_dir)