import json
import logging
import time
from pathlib import Path
from typing import Set, Optional
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from docpro_agent.core.client import DocProClient
from docpro_agent.config import Config
from docpro_agent.utils.helpers import wait_for_stable_file, sha256_path

logger = logging.getLogger("DocProAgent.WatcherService")

def is_temp_or_hidden(filename_str: str) -> bool:
    lower = filename_str.lower()
    return lower.startswith("~$") or lower.startswith(".") or lower.endswith((".tmp", ".temp", ".part"))

class DocumentUploadHandler(FileSystemEventHandler):
    def __init__(self, client: DocProClient, cfg: Config):
        self.client = client
        self.cfg = cfg
        self.uploaded_in_session: Set[str] = set()
        self.allowed_exts = {e.lower() for e in cfg.allowed_extensions}

    def on_created(self, event):
        if not event.is_directory:
            self._handle_file(event.src_path)

    def on_modified(self, event):
        if not event.is_directory:
            self._handle_file(event.src_path)

    def _handle_file(self, src_path: str) -> None:
        path = Path(src_path).resolve()
        path_str = str(path)
        if path_str in self.uploaded_in_session:
            return

        if is_temp_or_hidden(path.name):
            return
        
        # We now ALLOW files in 'output' because they are bundle results
        if path.suffix.lower() not in self.allowed_exts:
            return

        # Process
        if not wait_for_stable_file(path, self.cfg.stable_seconds):
            return
            
        if not path.exists():
            return

        # Exclusion Logic: 
        # 1. Ignore the source file download (bundle.pdf)
        # 2. Ignore anything in the 'input' directory
        if path.name.lower() == "bundle.pdf":
            return
        if "input" in [p.name.lower() for p in path.parents]:
            return

        manifest = self._find_manifest(path)
        if manifest:
            # 📝 CONFLICT AVOIDANCE: Check for lock file from main.py
            # If the folder has .processing_active, the global watcher stays silent.
            search_path = path.absolute()
            for _ in range(4):
                search_path = search_path.parent
                if (search_path / ".processing_active").exists():
                    logger.debug(f"Skipping global watch for {path.name}: Loop processor is active.")
                    return

            bundle_id = manifest.get("bundle_id")
            job_id = manifest.get("job_id")
            document_id = manifest.get("document_id")
            bundle_index = manifest.get("bundle_index")
            
            payload = {
                "job_id": str(job_id) if job_id else "",
                "bundle_id": str(bundle_id) if bundle_id else "",
                "document_id": str(document_id) if document_id else "",
                "bundle_index": bundle_index if bundle_index else 0,
                "sha256": sha256_path(path),
            }
            logger.info(f"Detected bundle result: {path.name} (Manifest linked)")
            logger.info(f"  → Metadata: Job={payload['job_id']}, Doc={payload['document_id']}, Bundle={payload['bundle_id']}")
            file_key = "result_pdf"
        else:
            # If it's in a 'Jobs' subfolder but no manifest, it might be internal - skip
            if "jobs" in [p.name.lower() for p in path.parents]:
                 logger.debug(f"Skipping file in Jobs folder with no manifest: {path.name}")
                 return

            logger.info(f"Detected new document for import: {path.name}")
            payload = {}
            file_key = "file"

        success = self.client.upload_document(path, payload, file_key)
        if success:
            self.uploaded_in_session.add(path_str)
            logger.info(f"  ✔ Submission complete: {path.name}")

    def _find_manifest(self, file_path: Path) -> Optional[dict]:
        search_path = file_path.absolute()
        # Look up to 4 levels higher for a manifest.json
        for _ in range(4):
            search_path = search_path.parent
            m_path = search_path / "manifest.json"
            if m_path.exists():
                try:
                    return json.loads(m_path.read_text(encoding="utf-8"))
                except Exception:
                    continue
            if search_path == search_path.parent: break
        return None

class WatcherService:
    def __init__(self, client: DocProClient, cfg: Config):
        self.client = client
        self.cfg = cfg
        self.observer = Observer()
        self.handler = DocumentUploadHandler(client, cfg)

    def start(self):
        roots = self.cfg.global_drop_dir
        if not roots:
            logger.warning("No global_drop_dir configured; Watcher service disabled.")
            return False

        paths = [Path(roots)] if isinstance(roots, str) else [Path(p) for p in roots]
        scheduled = False
        for p in paths:
            if not p.exists():
                p.mkdir(parents=True, exist_ok=True)
            # Use recursive=True to catch subfolders (Jobs/uuid/output)
            self.observer.schedule(self.handler, str(p), recursive=True)
            logger.info(f"Global Watcher started on: {p.absolute()}")
            scheduled = True

        if scheduled:
            self.observer.start()
            return True
        return False

    def stop(self):
        self.observer.stop()
        self.observer.join()
        logger.info("Watcher service stopped.")
