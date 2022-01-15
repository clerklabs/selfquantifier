#!/usr/bin/env bash
set -e
# set -x

# Set recommended conda config
conda config --set channel_priority flexible
conda config --set pip_interop_enabled True
conda config --set env_prompt '({name}) ' # https://stackoverflow.com/questions/56619347/anaconda-environment-bash-prefix-too-long

FOLDER="venv$1"

# Create a fresh local Python 3.7 (conda-pack does not support 3.8 yet) venv based on the python-clerkai environment
if [[ -d $FOLDER ]]; then
  rm -rf $FOLDER
fi
conda env create python==3.7 --prefix $FOLDER --file=environment.bare.yml

# Activate the environment
eval "$(conda shell.bash hook)"
conda activate ./$FOLDER

# Install additional pip-managed requirements
pip install -r requirements.txt

if [ "$1" == "dev" ]; then

  # Install dev requirements
  pip install -r requirements_dev.txt

  # For receipts parsing
  # brew install opencv || true
  # brew install poppler || true

  # For pytesseract
  # brew install tesseract || true

fi

# Replace the PyPI packages with its conda counterpart (where possible)
conda update --all --yes

# Update the yml
./export-conda-environment-yml.sh $FOLDER > environment$1.yml

echo "* Success: To activate the python venv, run"
echo "    conda activate ./$FOLDER"
