import shutil
import logging
from pathlib import Path
from typing import Optional

logger = logging.getLogger("DocProAgent.Workspace")

class WorkspaceManager:
    def __init__(self, base_dir: Path, bundle_id: str):
        self.root = base_dir / bundle_id
        self.input_dir = self.root / "input"
        self.output_dir = self.root / "output"
        self.final_dir = self.root / "final"

    def setup(self, bundle_id: str, job_id: str = "", document_id: str = "", bundle_index: int = 0) -> None:
        """Create the directory structure and the manifest for tracking."""
        for d in (self.input_dir, self.output_dir, self.final_dir):
            d.mkdir(parents=True, exist_ok=True)
        
        # Save a manifest for the global watcher to identify this folder
        # Ensure metadata keys are strings to prevent malformed URLs.
        manifest_path = self.root / "manifest.json"
        import json
        import time
        with manifest_path.open("w", encoding="utf-8") as f:
            json.dump({
                "bundle_id": str(bundle_id),
                "job_id": str(job_id),
                "document_id": str(document_id),
                "bundle_index": int(bundle_index),
                "created_at": time.time()
            }, f, indent=2)
        logger.info(f"Initialized workspace manifest at {manifest_path}")

    def cleanup(self) -> None:
        shutil.rmtree(self.root, ignore_errors=True)

    def locate_source_pdf(self) -> Optional[Path]:
        for candidate in (
            self.input_dir / "bundle.pdf",
            self.root / "bundle.pdf",
            *self.input_dir.glob("*.pdf"),
        ):
            if candidate.exists():
                return candidate
        return None
