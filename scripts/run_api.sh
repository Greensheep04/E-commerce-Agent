#!/usr/bin/env bash
set -euo pipefail

project_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$project_dir"
exec .venv/bin/uvicorn shop.app:app --host 127.0.0.1 --port 8000
