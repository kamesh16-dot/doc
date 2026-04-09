# DocPro - Document Processing Platform

A professional Django-based platform for document OCR, layout analysis, and data extraction.

## Project Structure

- `manage.py`: Project entry point.
- `config/`: Django settings, URLs, ASGI, and WSGI configuration.
- `apps/`: Core business logic apps (accounts, documents, processing, audit).
- `common/`: Shared utilities and enums.
- `static/`: Frontend static assets (CSS, JS, Images).
- `templates/`: Django HTML templates.
- `media/`: User-uploaded documents and processed files.
- `scripts/`: Maintenance and utility scripts.
- `requirements.txt`: Project dependencies.

## Setup on a New Laptop (Production Ready)

Follow these steps to get the project running seamlessly:

### 1. Prerequisites
- **Python 3.11+**
- **PostgreSQL**: Install and create a database named `doc1`.
- **Redis**: Install and start the Redis server (required for Celery).
- **Tesseract OCR**: Install the Tesseract engine on your system.

### 2. Environment Setup
```bash
# Clone/Open the project folder
cd DOCPROM-1

# Create and activate virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 3. Configuration
Copy `.env.example` to `.env` and fill in your credentials:
```bash
cp .env.example .env
# Edit .env with your DB_PASSWORD, OCR keys, etc.
```

### 4. Run the Application
Use the provided automation scripts for a seamless start:

- **Web Server**: `./run_web.sh` (Runs migrations, collects static, and starts Gunicorn)
- **Celery Worker**: `./run_worker.sh` (Handles background OCR/Analysis tasks)
- **Celery Beat**: `./run_beat.sh` (Handles scheduled maintenance)

## Project Structure
... (Refer to previous README content)
