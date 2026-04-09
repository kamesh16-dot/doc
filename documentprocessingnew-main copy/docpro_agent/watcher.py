import time
import logging
from docpro_agent.config import Config
from docpro_agent.core.client import DocProClient
from docpro_agent.services.watcher_service import WatcherService

logger = logging.getLogger("DocProAgent.Watcher")

def run_watcher():
    """Entry point for the watcher service."""
    logger.info("Initializing Watcher service...")
    try:
        cfg = Config.load()
        client = DocProClient(cfg)
        watcher = WatcherService(client, cfg)
        
        if watcher.start():
            logger.info("Watcher started. To stop, press Ctrl+C or exit from tray.")
            # Keep alive if running standalone
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                watcher.stop()
        else:
            logger.error("Watcher failed to start.")
    except Exception as e:
        logger.exception(f"Watcher crashed: {e}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_watcher()