import os
import subprocess
import logging
from pathlib import Path
from typing import Optional

logger = logging.getLogger("DocProAgent.ABBYY")

_ABBYY_FALLBACKS = [
    r"C:\Program Files\ABBYY FineReader 16\FineReader.exe",
    r"C:\Program Files\ABBYY FineReader 15\FineReader.exe",
    r"C:\Program Files (x86)\ABBYY FineReader 14\FineReaderOCR.exe",
    r"C:\Program Files (x86)\ABBYY FineReader 14\FineReader.exe",
]

class ABBYYLauncher:
    def __init__(self, configured_exe: str = ""):
        self.configured_exe = configured_exe

    def _resolve_exe(self) -> Optional[Path]:
        # 1. Configured override
        if self.configured_exe and Path(self.configured_exe).exists():
            return Path(self.configured_exe)

        # 2. Registry lookup
        try:
            import winreg
            # Check "App Paths" first
            keys = [
                (winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\Microsoft\Windows\CurrentVersion\App Paths\FineReader.exe"),
                (winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\Microsoft\Windows\CurrentVersion\App Paths\FineReaderOCR.exe"),
                (winreg.HKEY_CURRENT_USER, r"SOFTWARE\Microsoft\Windows\CurrentVersion\App Paths\FineReader.exe"),
            ]
            for hkey, subkey in keys:
                try:
                    with winreg.OpenKey(hkey, subkey) as key:
                        val, _ = winreg.QueryValueEx(key, "")
                        if val and Path(val).exists():
                            logger.info(f"Detected ABBYY via Registry: {val}")
                            return Path(val)
                except OSError:
                    continue
        except ImportError:
            pass

        # 3. Fallback hardcoded paths
        for c in _ABBYY_FALLBACKS:
            p = Path(c)
            if p.exists():
                logger.info(f"Detected ABBYY via Fallback: {p}")
                return p

        return None

    def launch(self, pdf_path: Path) -> bool:
        if not pdf_path.exists():
            logger.error(f"Source PDF not found: {pdf_path}")
            return False

        exe = self._resolve_exe()
        
        # Clean the input path
        path_str = str(pdf_path).strip('"')
        
        # We want ABBYY to treat the WORKSPACE ROOT as the context
        # PDF is in ws.root / job_id / input / bundle.pdf
        # We want cwd to be ws.root / job_id 
        ws_context = pdf_path.parent.parent
        if not ws_context.exists():
            ws_context = pdf_path.parent

        if exe:
            logger.info(f"Launching ABBYY: {exe}")
            try:
                # Use subprocess.Popen with a list for better path handling on Windows
                # We wrap path_str in quotes just in case, though Popen with list usually handles it.
                subprocess.Popen([str(exe), path_str], cwd=str(ws_context))
                return True
            except Exception as exc:
                logger.warning(f"Direct list-launch failed: {exc}. Trying shell string…")
                try:
                    subprocess.Popen(f'"{exe}" "{path_str}"', shell=True, cwd=str(ws_context))
                    return True
                except Exception as e2:
                    logger.warning(f"Shell launch failed: {e2}. Trying os.startfile…")
        
        logger.warning("ABBYY not found or failed to launch – using system default (os.startfile).")
        try:
            os.startfile(path_str)
            return True
        except Exception as exc:
            logger.error(f"os.startfile failed: {exc}")
            return False

