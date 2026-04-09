#!/bin/bash

# Activate virtual environment if it exists
if [ -d ".venv" ]; then
    source .venv/bin/activate
fi

# Start Celery Worker
echo "Starting Celery Worker..."
celery -A config worker --loglevel=info
