#!/bin/bash

# Activate virtual environment
source .venv/bin/activate

# Set environment variables
export PYTHONPATH=$PYTHONPATH:$(pwd)

# Create logs directory if it doesn't exist
mkdir -p logs

# Run uvicorn with proper configuration
uvicorn wsgi:app \
      --host 0.0.0.0 \
      --port 443 \
      --ssl-certfile /etc/letsencrypt/live/explore.ivrit.ai/fullchain.pem \
      --ssl-keyfile /etc/letsencrypt/live/explore.ivrit.ai/privkey.pem \
      --workers 2 \
      --log-level info \
      --access-log \
      --log-config logging.conf 2>&1 | tee logs/uvicorn.log
