#!/bin/bash

# Activate virtual environment if it exists
if [ -d ".venv" ]; then
    source .venv/bin/activate
fi

# Start Celery Beat
echo "Starting Celery Beat..."
celery -A config beat --loglevel=info
