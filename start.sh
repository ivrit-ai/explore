#!/bin/bash

# Activate virtual environment
source .venv/bin/activate

# Set environment variables
export PYTHONPATH=$PYTHONPATH:$(pwd)

# Create logs directory if it doesn't exist
mkdir -p logs

# Run uvicorn via run.py (which does eager init of index, etc.)
# HTTP only — SSL is handled by the reverse proxy.
# To serve audio from S3 instead of the local audio/ dir, add:
#   --audio-source s3 --s3-bucket <bucket>
# (or set EXPLORE_AUDIO_SOURCE / EXPLORE_S3_BUCKET in the environment).
python run.py --data-dir /home/data/explore --port 8200 2>&1 | tee logs/uvicorn.log
