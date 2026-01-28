#!/bin/bash

# Activate virtual environment
source .venv/bin/activate

# Set environment variables
export PYTHONPATH=$PYTHONPATH:$(pwd)
export FLASK_APP=run.py

# Create logs directory if it doesn't exist
mkdir -p logs

# Run uwsgi with proper configuration
uwsgi --master \
      --https 0.0.0.0:443,/etc/letsencrypt/live/explore.ivrit.ai/fullchain.pem,/etc/letsencrypt/live/explore.ivrit.ai/privkey.pem \
      --module run:app \
      --pyargv "--data-dir /root/data" \
      --logto logs/uwsgi.log \
      --log-date \
      --log-4xx \
      --log-5xx \
      --enable-threads \
      --threads 4 \
      --processes 2 \
      --harakiri 30 \
      --harakiri-verbose \
      --socket-timeout 30 \
      --http-timeout 30
