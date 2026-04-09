import json
import os
import sys
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional, Union
from docpro_agent.utils.paths import get_exe_dir, base_path

logger = logging.getLogger("DocProAgent.Config")

@dataclass
class Config:
    api_base: str
    device_id: str
    api_token: str
    download_dir: Path
    abbyy_exe: str = ""
    poll_seconds: int = 5
    stable_seconds: int = 10
    watcher_timeout_seconds: int = 3600
    merge_wait_seconds: int = 120
    global_drop_dir: Optional[Union[str, List[str]]] = None
    allowed_extensions: List[str] = field(default_factory=lambda: [".docx", ".pdf"])

    @staticmethod
    def load(config_path: Optional[Path] = None) -> "Config":
        if not config_path:
            # Beside the EXE is the default for production config
            config_path = get_exe_dir() / "config.json"
        
        if not config_path.exists():
            # Try to bootstrap from example if we're in the dev environment or bundled
            # In a frozen bundle, config.example.json should be in the root of sys._MEIPASS
            # In source mode, it's in docpro_agent/config.example.json
            example_path = base_path("docpro_agent/config.example.json")
            if not example_path.exists():
                # Fallback for frozen mode where the path might be flatter
                example_path = base_path("config.example.json")
            
            if example_path.exists():
                import shutil
                shutil.copy(example_path, config_path)
                logger.info(f"Bootstrapped config from {example_path} to {config_path}")
            else:
                raise FileNotFoundError(f"config.json not found and bootstrap example missing at {example_path}")

        with open(config_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        # Required check
        for req in ["api_base", "device_id", "api_token", "download_dir"]:
            if req not in data:
                raise ValueError(f"Config is missing required field: {req}")

        # Resolve download_dir relative to the EXE if not absolute
        dl_path = Path(data["download_dir"])
        if not dl_path.is_absolute():
            dl_path = get_exe_dir() / dl_path

        return Config(
            api_base=data["api_base"].rstrip("/"),
            device_id=data["device_id"],
            api_token=data["api_token"],
            download_dir=dl_path,
            abbyy_exe=data.get("abbyy_exe", ""),
            poll_seconds=int(data.get("poll_seconds", 5)),
            stable_seconds=int(data.get("stable_seconds", 10)),
            watcher_timeout_seconds=int(data.get("watcher_timeout_seconds", 3600)),
            merge_wait_seconds=int(data.get("merge_wait_seconds", 120)),
            global_drop_dir=data.get("global_drop_dir"),
            allowed_extensions=data.get("allowed_extensions", [".docx", ".pdf"]),
        )
