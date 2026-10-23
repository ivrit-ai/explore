#!/usr/bin/env bash
# Deploy ivrit.ai Explore on the server: update code, install deps, build the UI.
# Usage: ./deploy.sh [branch]   (default: main)
# Settings stay in .env on the server (see .env.example). The Docker image (see
# Dockerfile) is the hosted deployment; this script is for running from a checkout.
set -euo pipefail

cd "$(dirname "$0")"
BRANCH="${1:-main}"

node_major="$(node -p 'process.versions.node.split(".")[0]' 2>/dev/null || echo 0)"
if [ "$node_major" -lt 20 ]; then
  echo "Node.js 20+ is required to build the frontend (found: $(node -v 2>/dev/null || echo none))" >&2
  exit 1
fi

echo "==> Updating code ($BRANCH)"
git fetch origin "$BRANCH"
git checkout "$BRANCH"
git pull --ff-only origin "$BRANCH"

echo "==> Installing Python dependencies"
# shellcheck disable=SC1091
source .venv/bin/activate
pip install --quiet -r requirements.txt

echo "==> Building frontend"
(cd frontend && npm ci --no-audit --no-fund && npm run build)

echo "==> Done. Restart the app (e.g. stop it and run ./start.sh) to serve the new build."
