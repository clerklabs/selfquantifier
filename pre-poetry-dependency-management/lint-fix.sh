set -x
eval "$(conda shell.bash hook)"
conda activate ./venvdev
black --exclude 'venv|venvdev|.eggs' .
python -m isort -rc --skip venv,venvdev .
python -m autopep8 -r --in-place --exclude venv,venvdev .
