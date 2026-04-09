import logging
import uuid
import requests
import zipfile
from functools import wraps
from typing import Callable, Optional
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pathlib import Path
from docpro_agent.config import Config

logger = logging.getLogger("DocProAgent.Client")

def retry(
    max_attempts: int = 3,
    delay: float = 2.0,
    backoff: float = 2.0,
    exceptions: tuple = (requests.RequestException,),
):
    def decorator(fn: Callable) -> Callable:
        @wraps(fn)
        def wrapper(*args, **kwargs):
            import time
            wait = delay
            for attempt in range(1, max_attempts + 1):
                try:
                    return fn(*args, **kwargs)
                except exceptions as exc:
                    if attempt == max_attempts:
                        logger.error(f"{fn.__name__} failed after {max_attempts} attempts: {exc}")
                        raise
                    logger.warning(
                        f"{fn.__name__} attempt {attempt}/{max_attempts} failed: {exc}. "
                        f"Retrying in {wait:.1f}s…"
                    )
                    time.sleep(wait)
                    wait *= backoff
        return wrapper
    return decorator

class DocProClient:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.s = self._build_session(cfg.api_token)
        self.agent_version = "1.2.0" # Updated version for unified agent

    def _build_session(self, token: str) -> requests.Session:
        session = requests.Session()
        session.headers.update({"Authorization": f"Token {token}"})
        adapter = HTTPAdapter(
            max_retries=Retry(
                total=3,
                backoff_factor=1,
                status_forcelist=[500, 502, 503, 504],
                allowed_methods=["GET", "POST"],
            )
        )
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def _url(self, *parts: str) -> str:
        return "/".join([self.cfg.api_base, self.cfg.device_id, *parts]).rstrip("/") + "/"

    @staticmethod
    def _machine_id() -> str:
        return str(uuid.getnode())

    @retry(max_attempts=3, delay=2)
    def heartbeat(self) -> None:
        r = self.s.post(
            self._url("heartbeat"),
            json={"agent_version": self.agent_version, "machine_id": self._machine_id()},
            timeout=10,
        )
        r.raise_for_status()

    @retry(max_attempts=3, delay=2)
    def next_bundle(self) -> Optional[dict]:
        r = self.s.get(self._url("next-bundle"), timeout=20)
        r.raise_for_status()
        return r.json().get("bundle")

    @retry(max_attempts=2, delay=3)
    def claim_bundle(self, bundle_id: str) -> Optional[dict]:
        logger.info(f"Claiming bundle {bundle_id}…")
        r = self.s.post(self._url("bundles", bundle_id, "acquire"), timeout=20)
        if r.status_code == 403:
            logger.error("Claim denied: no assignment for this device.")
            return None
        if r.status_code == 409:
            logger.error("Claim denied: bundle already leased to another device.")
            return None
        r.raise_for_status()
        return r.json().get("bundle")

    @retry(max_attempts=3, delay=5)
    def download_bundle(self, bundle_id: str, dest_dir: Path) -> Path:
        logger.info(f"Downloading bundle {bundle_id}…")
        r = self.s.get(self._url("bundles", bundle_id, "download"), timeout=300)
        r.raise_for_status()
        dest_dir.mkdir(parents=True, exist_ok=True)
        zip_path = dest_dir / "bundle.zip"
        zip_path.write_bytes(r.content)
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(dest_dir)
        zip_path.unlink()
        return dest_dir

    @retry(max_attempts=3, delay=3)
    def upload_page(self, bundle_id: str, page_number: int, file_path: Path) -> dict:
        logger.info(f"  ↑ Uploading page {page_number} ({file_path.name})")
        with file_path.open("rb") as fh:
            r = self.s.post(
                self._url("bundles", bundle_id, "upload-page"),
                files={"page_file": (file_path.name, fh)},
                data={"page_number": page_number},
                timeout=300,
            )
        r.raise_for_status()
        return r.json()

    @retry(max_attempts=5, delay=5)
    def check_document_status(self, bundle_id: str) -> dict:
        r = self.s.get(self._url("bundles", bundle_id, "status"), timeout=10)
        r.raise_for_status()
        return r.json()

    @retry(max_attempts=3, delay=5)
    def upload_document(self, file_path: Path, payload: dict, file_key: str = "file") -> bool:
        """Upload a document to the backend (standard import or bundle result)."""
        filename = file_path.name
        bundle_id = payload.get("bundle_id")
        
        if bundle_id:
            # Result for a specific bundle
            url = self._url("bundles", str(bundle_id), "upload-result")
        else:
            # General document import
            url = self.cfg.api_base.rstrip("/")
            if "/api/v1" not in url:
                url += "/api/v1"
            url += "/documents/"

        # Safety check: Prevent malformed URLs (double slashes)
        if "//upload-result" in url:
            logger.error(f"Abort upload: Malformed URL detected for {filename}. Payload: {payload}")
            return False

        logger.info(f"Uploading {filename} to {url}...")
        try:
            with file_path.open("rb") as f:
                r = self.s.post(
                    url,
                    files={file_key: (filename, f)},
                    data=payload,
                    timeout=600,
                )
            if r.status_code in {200, 201}:
                logger.info(f"  ✔ {filename} uploaded successfully.")
                return True
            else:
                # Include full response text for easier debugging of backend errors
                logger.error(f"  ✘ Upload failed ({r.status_code}): {r.text}")
                return False
        except Exception as e:
            logger.error(f"  ✘ Request failed for {filename}: {e}")
            return False
