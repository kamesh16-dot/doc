import hashlib
import time
from pathlib import Path
import logging

logger = logging.getLogger("DocProAgent.Utils")

def sha256_path(path: Path) -> str:
    """Compute SHA256 hash of a file."""
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def wait_for_stable_file(path: Path, stable_seconds: int = 3, interval: float = 1.0) -> bool:
    """
    Wait until file size remains unchanged for `stable_seconds`.
    Returns True if stable, False if file disappears or inaccessible.
    """
    last_size = -1
    stable_for = 0.0

    while stable_for < stable_seconds:
        try:
            if not path.exists():
                return False
            
            size = path.stat().st_size
            if size > 0 and size == last_size:
                stable_for += interval
            else:
                stable_for = 0.0
                last_size = size
        except (OSError, PermissionError):
            # File might be locked or inaccessible
            stable_for = 0.0
        
        time.sleep(interval)
    
    return True
