import os
import sys
import threading
import signal
import time
import logging
from pathlib import Path
from PIL import Image, ImageDraw
import pystray
from pystray import MenuItem as item
import winshell
from win32com.client import Dispatch
import win32event
import win32api
import winerror

import tkinter as tk
from tkinter import ttk, scrolledtext

# Windows Mutex to allow the installer to detect the running app
# This must match the AppMutex in installer.iss
_instance_mutex = win32event.CreateMutex(None, False, "DocProAgent_Instance_Mutex")
if win32api.GetLastError() == winerror.ERROR_ALREADY_EXISTS:
    if getattr(sys, 'frozen', False):
        # Already running: Forward arguments to the existing instance via socket
        if len(sys.argv) > 1:
            import socket
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.settimeout(2)
                    s.connect(('127.0.0.1', 5555))
                    s.sendall(sys.argv[1].encode('utf-8'))
            except: pass
        sys.exit(0)

from docpro_agent.config import Config
from docpro_agent.core.client import DocProClient
from docpro_agent.services.watcher_service import WatcherService
from docpro_agent.main import run_agent, process_bundle_id
from docpro_agent.utils.paths import base_path, get_exe_dir

# Logging setup
_log_queue = []
class LogQueueHandler(logging.Handler):
    def emit(self, record):
        _log_queue.append(self.format(record))
        if len(_log_queue) > 100: _log_queue.pop(0)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(get_exe_dir() / "agent.log", encoding="utf-8"),
        LogQueueHandler()
    ]
)
logger = logging.getLogger("DocProAgent.App")

class DocProAgentApp:
    def __init__(self):
        self.stop_event = threading.Event()
        self.icon = None
        self.threads = []
        self.cfg = None
        self.root = None
        self.status_labels = {}
        self.log_text = None

    def create_placeholder_icon(self):
        icon_path = base_path("assets/app.ico")
        if not icon_path.exists():
            icon_path.parent.mkdir(parents=True, exist_ok=True)
            img = Image.new('RGB', (64, 64), color=(0, 120, 215))
            d = ImageDraw.Draw(img)
            d.text((20, 15), "D", fill=(255, 255, 255))
            img.save(icon_path)
        return icon_path

    def setup_autostart(self):
        if not getattr(sys, 'frozen', False): return
        startup_path = Path(winshell.startup()) / "DocProAgent.lnk"
        if not startup_path.exists():
            try:
                shell = Dispatch('WScript.Shell')
                shortcut = shell.CreateShortCut(str(startup_path))
                shortcut.Targetpath = sys.executable
                shortcut.WorkingDirectory = str(Path(sys.executable).parent)
                shortcut.IconLocation = sys.executable
                shortcut.Description = "DocPro Desktop Agent"
                shortcut.save()
            except Exception as e:
                logger.error(f"Autostart error: {e}")

    def on_exit(self, icon=None, item=None):
        logger.info("Shutdown requested.")
        self.stop_event.set()
        if self.icon: self.icon.stop()
        if self.root: self.root.quit()

    def show_window(self, icon=None, item=None):
        if self.root:
            self.root.deiconify()
            self.root.lift()
            self.root.focus_force()

    def hide_window(self):
        if self.root:
            self.root.withdraw()
            try:
                self.icon.notify("Agent is still running in the tray.", title="DocPro Agent")
            except: pass

    def run_tray(self):
        icon_path = self.create_placeholder_icon()
        image = Image.open(icon_path)
        menu = pystray.Menu(
            item('Open Dashboard', self.show_window, default=True),
            item('Exit', self.on_exit)
        )
        self.icon = pystray.Icon("DocProAgent", image, "DocPro Agent", menu)
        self.icon.run_detached()

    def build_gui(self):
        self.root = tk.Tk()
        self.root.title("DocPro Agent Dashboard")
        self.root.geometry("600x450")
        self.root.protocol("WM_DELETE_WINDOW", self.hide_window)
        
        # Icon
        try:
            icon_path = self.create_placeholder_icon()
            self.root.iconbitmap(str(icon_path))
        except: pass

        # Layout
        frame = ttk.Frame(self.root, padding="10")
        frame.pack(fill=tk.BOTH, expand=True)

        ttk.Label(frame, text="DocPro Agent - System Status", font=("Helvetica", 14, "bold")).pack(pady=5)
        
        status_frame = ttk.LabelFrame(frame, text="Services", padding="10")
        status_frame.pack(fill=tk.X, pady=10)

        self.status_labels['agent'] = ttk.Label(status_frame, text="Agent: 🟢 Polling for work", foreground="green")
        self.status_labels['agent'].pack(fill=tk.X, pady=2)
        
        self.status_labels['current_task'] = ttk.Label(status_frame, text="Task: Idle", foreground="gray")
        self.status_labels['current_task'].pack(fill=tk.X, pady=2)

        ttk.Label(frame, text="Activity Log:").pack(anchor=tk.W, pady=(10, 0))
        self.log_text = scrolledtext.ScrolledText(frame, height=12, state='disabled', font=("Consolas", 9))
        self.log_text.pack(fill=tk.BOTH, expand=True, pady=5)

        btn_frame = ttk.Frame(frame)
        btn_frame.pack(fill=tk.X, pady=5)
        
        ttk.Button(btn_frame, text="Hide to Tray", command=self.hide_window).pack(side=tk.LEFT, padx=5)
        
        # New: Manual Scan button in case auto-detection misses something
        def manual_scan():
            logger.info("Manual scan triggered by user.")
            # This doesn't do much on its own but logs the intent
        ttk.Button(btn_frame, text="Force Scan", command=manual_scan).pack(side=tk.LEFT, padx=5)

        
        # Update log loop
        self._update_log_view()

    def _update_log_view(self):
        if self.log_text and not self.stop_event.is_set():
            if _log_queue:
                self.log_text.config(state='normal')
                while _log_queue:
                    self.log_text.insert(tk.END, _log_queue.pop(0) + "\n")
                self.log_text.see(tk.END)
                self.log_text.config(state='disabled')
            self.root.after(500, self._update_log_view)

    def _update_status(self, key: str, text: str):
        """Update a status label safely from any thread."""
        def _set():
            if key in self.status_labels:
                lbl = self.status_labels[key]
                lbl.config(text=text)
                if text.startswith("🔴"):
                    lbl.config(foreground="red")
                elif text.startswith("🟢"):
                    lbl.config(foreground="green")
                elif text.startswith("🟡"):
                    lbl.config(foreground="orange")
                else:
                    lbl.config(foreground="black")
        self.root.after(0, _set)

    def start_services(self):
        try:
            self.cfg = Config.load()
        except: return

        # Threads
        agent_thread = threading.Thread(target=self._safe_run, args=(lambda: run_agent(self.stop_event),), daemon=True)
        agent_thread.start()
        self.threads.append(agent_thread)

        watcher_thread = threading.Thread(target=self._safe_run, args=(self.run_watcher_service,), daemon=True)
        watcher_thread.start()
        self.threads.append(watcher_thread)

    def run_watcher_service(self):
        client = DocProClient(self.cfg)
        watcher = WatcherService(client, self.cfg)
        if watcher.start():
            self.stop_event.wait()
            watcher.stop()

    def _safe_run(self, func):
        try: func()
        except Exception as e: logger.exception(f"Thread error: {e}")

    def main(self):
        self.setup_autostart()
        self.start_services()
        
        # Start IPC Listener for protocol triggers
        threading.Thread(target=self._run_ipc_listener, daemon=True).start()
        
        # Check for initial protocol launch
        if len(sys.argv) > 1:
            self._handle_protocol_arg(sys.argv[1])

        self.run_tray()
        self.build_gui()
        self.root.mainloop()

    def _handle_protocol_arg(self, arg):
        arg = arg.lower()
        if arg.startswith("docpro://open/"):
            bundle_id = arg.split("/")[-1].rstrip("/")
            if bundle_id:
                logger.info(f"Protocol trigger received for bundle: {bundle_id}")
                # Pass self._update_status callback to process_bundle_id
                t = threading.Thread(
                    target=self._safe_run, 
                    args=(lambda: process_bundle_id(bundle_id, self.stop_event, status_callback=self._update_status),), 
                    daemon=True
                )
                t.start()
                self.threads.append(t)

    def _run_ipc_listener(self):
        import socket
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind(('127.0.0.1', 5555))
                s.listen(5)
                while not self.stop_event.is_set():
                    conn, _ = s.accept()
                    with conn:
                        data = conn.recv(1024)
                        if data:
                            arg = data.decode('utf-8')
                            self._handle_protocol_arg(arg)
        except Exception as e:
            logger.debug(f"IPC Listener error (likely port in use): {e}")

def run_app_safely():
    try:
        app = DocProAgentApp()
        app.main()
    except Exception as e:
        import ctypes, traceback
        ctypes.windll.user32.MessageBoxW(0, f"DocPro Agent Error:\n{e}\n{traceback.format_exc()}", "Startup Error", 0x10)
        sys.exit(1)

if __name__ == "__main__":
    run_app_safely()
