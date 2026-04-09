# DocPro Desktop Agent (Unified)

The DocPro Agent is a unified background service for Windows that combines job processing and file system monitoring into a single, high-performance application.

## 🚀 Key Features
- **Unified Entry Point**: Managed through `app.py` (or the compiled `DocProInstaller.exe`).
- **Multi-Threaded**: Runs its components (Agent and Watcher) as concurrent background threads.
- **System Tray Control**: Runs silently in the background with a system tray icon for status and exit.
- **Automatic Startup**: Registers itself as a Windows startup application on the first launch.
- **Dynamic OCR Detection**: Automatically locates ABBYY FineReader versions 14, 15, or 16.

## 🛠️ Installation & Usage

### 📦 Using the Installer (Recommended)
1.  Navigate to `docpro_agent/dist/DocProInstaller.exe`.
2.  Run the installer to set up the agent as a local Windows application.
3.  The agent will launch automatically and appear in your system tray.

### 🐍 Running from Source (Developer)
1.  Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```
2.  Launch the unified agent:
    ```bash
    python -m docpro_agent.app
    ```

## 🏗️ Build Instructions

### 1. Build Executable
To create a standalone `.exe` using PyInstaller:
```powershell
./docpro_agent/build.ps1
```

### 2. Generate Installer
To create the professional Windows Installer (requires Inno Setup):
```powershell
& "C:\Users\ADMIN\AppData\Local\Programs\Inno Setup 6\ISCC.exe" docpro_agent/installer.iss
```

## 📄 Configuration (`config.json`)
The application auto-generates `config.json` from `config.example.json` if missing.
- `api_base`: Base URL of the DocPro server.
- `device_id`: Unique identifier for this computer.
- `download_dir`: Path where active work bundles are downloaded.
- `global_drop_dir`: Folder(s) monitored for automatic document ingestion.
