import sys
import os
from pathlib import Path

def base_path(relative: str = "") -> Path:
    """
    Returns the base path for readonly assets (bundled with the EXE).
    """
    if hasattr(sys, '_MEIPASS'):
        # PyInstaller bundles files in a temporary directory
        base = Path(sys._MEIPASS).resolve()
    else:
        # Running as a script. Project root is the grandparent of this file's folder.
        # __file__ is docpro_agent/utils/paths.py -> parent is utils/ -> parent.parent is docpro_agent/
        base = Path(__file__).resolve().parent.parent.parent

    if relative:
        return (base / relative).resolve()
    return base

def get_exe_dir() -> Path:
    """
    Returns the directory of the current executable (or script).
    Mutable data (like config.json) should live here in production.
    """
    if getattr(sys, 'frozen', False):
        # We are in a bundle, use the directory of the .EXE
        return Path(sys.executable).parent.resolve()
    
    # Running as script, use the folder of the entry script (sys.argv[0])
    return Path(sys.argv[0]).resolve().parent

def get_data_path(relative: str = "") -> Path:
    """
    Returns a path for mutable data. Defaults to beside the EXE.
    """
    base = get_exe_dir()
    path = base / relative if relative else base
    path.mkdir(parents=True, exist_ok=True)
    return path
