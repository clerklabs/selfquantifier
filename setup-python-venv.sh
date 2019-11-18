#!/usr/bin/env bash
set -e
# set -x

# Set recommended conda config
conda config --set channel_priority flexible
conda config --set pip_interop_enabled True
conda config --set env_prompt '({name}) ' # https://stackoverflow.com/questions/56619347/anaconda-environment-bash-prefix-too-long

# Create and update a local Python 3 venv based on the python-clerkai environment
if [[ ! -d venv ]]; then
  conda env create python==3.7 --prefix venv --file=environment.yml
else
  conda env update --prefix venv --file=environment.yml
fi

# Activate the environment
eval "$(conda shell.bash hook)"
conda activate ./venv

# Install additional pip-managed requirements
pip install -r requirements.txt

# Replace the PyPI packages with its own (where possible)
conda update --all

# For receipts parsing
# brew install poppler || true
# brew install tesseract || true

echo "* Success: To activate the python venv, run"
echo "    conda activate ./venv"
