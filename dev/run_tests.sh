#!/usr/bin/env bash
set -euo pipefail

# Ensure src/ is on the import path for tests like: from ingest_data.ingest import ...
export PYTHONPATH=src

python -m pip install --upgrade pip
# If you have requirements.txt, install it (won't fail if missing)
if [ -f requirements.txt ]; then
  pip install -r requirements.txt
fi

# Optional: install your package in editable mode if you add a pyproject.toml later
# pip install -e .

# Run tests
python -m unittest discover -s tests -p "test*.py" -v
