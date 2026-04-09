import sys
import logging
import requests
from pathlib import Path

# Try to import project modules
try:
    from docpro_agent.config import Config
    from docpro_agent.core.client import DocProClient
    from docpro_agent.services.abbyy import ABBYYLauncher
    from docpro_agent.utils.paths import get_exe_dir
except ImportError as e:
    print(f"FAILED: Could not import project modules. Ensure you are running from the root. Error: {e}")
    sys.exit(1)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("CheckEnv")

def run_check():
    print("=== DocPro Agent Environment Check ===")
    
    # 1. Config Check
    print("\n1. Checking Config...")
    try:
        cfg = Config.load()
        print(f"  OK: Config loaded from {get_exe_dir() / 'config.json'}")
        print(f"  API Base: {cfg.api_base}")
        print(f"  Device ID: {cfg.device_id}")
    except Exception as e:
        print(f"  FAILED: Config error: {e}")
        return

    # 2. Server Connectivity
    print("\n2. Checking Server Connectivity...")
    try:
        client = DocProClient(cfg)
        client.heartbeat()
        print("  OK: Heartbeat successful.")
    except Exception as e:
        print(f"  FAILED: Could not reach server: {e}")

    # 3. ABBYY Detection
    print("\n3. Checking ABBYY FineReader...")
    launcher = ABBYYLauncher(cfg.abbyy_exe)
    exe_path = launcher._resolve_exe()
    if exe_path:
        print(f"  OK: ABBYY found at {exe_path}")
    else:
        print("  WARN: ABBYY not found in registry or common paths. Will use system default.")

    # 4. Permissions
    print("\n4. Checking Write Permissions...")
    try:
        test_file = cfg.download_dir / ".permission_test"
        cfg.download_dir.mkdir(parents=True, exist_ok=True)
        test_file.write_text("test")
        test_file.unlink()
        print(f"  OK: Write access to {cfg.download_dir}")
    except Exception as e:
        print(f"  FAILED: No write access to download_dir: {e}")

    print("\n=== Check Complete ===")

if __name__ == "__main__":
    run_check()
