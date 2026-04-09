#!/bin/bash

# Activate virtual environment if it exists
if [ -d ".venv" ]; then
    source .venv/bin/activate
fi

# Run migrations
python manage.py migrate

# Collect static files
python manage.py collectstatic --no-input

# Start Gunicorn
echo "Starting Gunicorn..."
gunicorn config.wsgi:application --bind 0.0.0.0:8000 --workers 3
