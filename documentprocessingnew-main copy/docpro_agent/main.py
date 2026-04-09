import logging
import time
import threading
from typing import Optional
from docpro_agent.config import Config
from docpro_agent.core.client import DocProClient
from docpro_agent.core.workspace import WorkspaceManager
from docpro_agent.services.abbyy import ABBYYLauncher
from docpro_agent.utils.helpers import wait_for_stable_file

logger = logging.getLogger("DocProAgent.Agent")

def process_bundle_id(bundle_id: str, stop_event: Optional[threading.Event] = None, status_callback: Optional[Callable[[str, str], None]] = None):
    """Acquires and processes a specific bundle by ID."""
    logger.info(f"Targeting specific bundle: {bundle_id}")
    if status_callback: status_callback("agent", "🟡 Acquiring bundle...")
    try:
        cfg = Config.load()
        client = DocProClient(cfg)
        launcher = ABBYYLauncher(cfg.abbyy_exe)
        
        bundle = client.claim_bundle(bundle_id)
        if bundle:
            process_bundle(client, launcher, cfg, bundle, status_callback)
        else:
            logger.error(f"Could not acquire bundle {bundle_id}")
            if status_callback: status_callback("agent", "🔴 Claim failed")
    except Exception as e:
        logger.exception(f"Error in specific bundle processing ({bundle_id}): {e}")
        if status_callback: status_callback("agent", "🔴 Error occurred")

def process_bundle(
    client: DocProClient,
    launcher: ABBYYLauncher,
    cfg: Config,
    bundle: dict,
    status_callback: Optional[Callable[[str, str], None]] = None
) -> bool:
    bundle_id = bundle["id"]
    job_id = bundle.get("job_id", "")
    document_id = bundle.get("document_id", "")
    bundle_index = bundle.get("bundle_index", 0)
    ws = WorkspaceManager(cfg.download_dir, bundle_id)
    
    # 📝 CREATE LOCK FILE for the Global Watcher
    lock_file = ws.root / ".processing_active"
    
    try:
        ws.setup(bundle_id, job_id, document_id, bundle_index)
        lock_file.touch(exist_ok=True)
        
        if status_callback: status_callback("agent", f"🟡 Processing {bundle_id[:8]}...")
        if status_callback: status_callback("current_task", f"Downloading bundle files...")
        client.download_bundle(bundle_id, ws.input_dir)
        source_pdf = ws.locate_source_pdf()
        if source_pdf is None:
            logger.error(f"bundle.pdf not found in {ws.input_dir}")
            return False

        if not launcher.launch(source_pdf):
            logger.error("Could not open PDF with ABBYY.")
            if status_callback: status_callback("agent", "🔴 ABBYY Launch Failed")
            return False

        if status_callback: status_callback("current_task", "Waiting for ABBYY Save...")

        # Watch output, upload pages
        logger.info(f"Watching {ws.root} and its subdirectories for results...")
        expected_pages = set(bundle.get("page_numbers", []))
        uploaded_files = set()
        deadline = time.time() + cfg.watcher_timeout_seconds
        
        while time.time() < deadline:
            # Recursive scan of the entire workspace root
            all_files = []
            if ws.root.exists():
                all_files.extend(list(ws.root.rglob("*")))
            
            for file_path in sorted(all_files):
                if not file_path.is_file() or file_path.name in uploaded_files:
                    continue
                
                # Exclusions: Skip input folder and source pdf
                parts = [p.name.lower() for p in file_path.parents]
                if "input" in parts or file_path.name.lower() == "bundle.pdf":
                    continue
                if file_path.name.lower() in ["manifest.json", ".processing_active"]:
                    continue
                
                # Hidden/Temp files
                if file_path.name.startswith("~") or file_path.name.startswith("."):
                    continue

                import re
                page_match = re.search(r"(\d+)", file_path.name)
                
                if page_match and expected_pages:
                    page_num = int(page_match.group(1))
                    if page_num not in expected_pages:
                        continue
                    
                    if wait_for_stable_file(file_path, cfg.stable_seconds):
                        try:
                            from docpro_agent.utils.helpers import sha256_path
                            payload = {
                                "bundle_id": bundle_id,
                                "job_id": job_id,
                                "document_id": document_id,
                                "bundle_index": bundle_index,
                                "page_number": page_num,
                                "sha256": sha256_path(file_path)
                            }
                            if client.upload_document(file_path, payload, "result_pdf"):
                                uploaded_files.add(file_path.name)
                                logger.info(f"Page {page_num} uploaded ({len(uploaded_files)}/{len(expected_pages)})")
                        except Exception as e:
                            logger.error(f"Failed to upload page {page_num}: {e}")
                else:
                    # Non-numeric result or result found when expected_pages is empty
                    if file_path.suffix.lower() in [".pdf", ".docx"]:
                        if wait_for_stable_file(file_path, cfg.stable_seconds):
                            logger.info(f"Detected general result file: {file_path.name}")
                            from docpro_agent.utils.helpers import sha256_path
                            payload = {
                                "bundle_id": bundle_id,
                                "job_id": job_id,
                                "document_id": document_id,
                                "bundle_index": bundle_index,
                                "sha256": sha256_path(file_path)
                            }
                            if client.upload_document(file_path, payload, "result_pdf"):
                                uploaded_files.add(file_path.name)
                                logger.info(f"General result submitted: {file_path.name}")
                                # If we uploaded a full result, we consider this bundle's work done
                                deadline = 0 
                                break

            # SUCCESS BREAK: We have all pages OR we just uploaded a full document
            if deadline == 0: break
            if expected_pages and len(uploaded_files) >= len(expected_pages):
                logger.info("All assigned pages uploaded.")
                break
            
            time.sleep(3) # Slightly longer sleep to reduce I/O pressure
            try: client.heartbeat()
            except: pass


        # Wait for reconstruction
        deadline = time.time() + cfg.merge_wait_seconds
        while time.time() < deadline:
            try:
                status = client.check_document_status(bundle_id)
                if status.get("is_complete"):
                    logger.info("Reconstruction complete.")
                    break
            except: pass
            time.sleep(5)
        
        if status_callback: 
            status_callback("agent", "🟢 Work Complete")
            status_callback("current_task", "Idle")

        return True
    except Exception as e:
        logger.exception(f"Error processing bundle {bundle_id}: {e}")
        return False

def run_agent(stop_event: Optional[threading.Event] = None):
    """Main agent loop."""
    logger.info("Initializing Agent service...")
    try:
        cfg = Config.load()
        client = DocProClient(cfg)
        launcher = ABBYYLauncher(cfg.abbyy_exe)
        
        logger.info("Polling for work (stop_event monitored)...")
        while not (stop_event and stop_event.is_set()):
            try:
                client.heartbeat()
                bundle = client.next_bundle()
                if bundle:
                    process_bundle(client, launcher, cfg, bundle)
                else:
                    # Sleep in small chunks to stay responsive to stop_event
                    for _ in range(cfg.poll_seconds * 2):
                        if stop_event and stop_event.is_set(): break
                        time.sleep(0.5)
            except Exception as e:
                logger.error(f"Polling error: {e}")
                time.sleep(cfg.poll_seconds)
    except Exception as e:
        logger.exception(f"Agent crashed: {e}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_agent()
