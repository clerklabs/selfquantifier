#!/usr/bin/env bash
set -e

# Create and update a local Python 3 venv
if [[ ! -d venv ]]; then
  conda env create python==3.7 --prefix venv --file=environment.yml
else
  conda env update --prefix venv --file=environment.yml
fi

# For receipts parsing
brew install poppler || true
brew install tesseract || true

echo "* Success: To activate the python venv, run"
echo "    conda activate ./venv"
