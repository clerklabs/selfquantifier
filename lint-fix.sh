set -x
eval "$(conda shell.bash hook)"
conda activate ./venv
black --exclude 'venv|.eggs' .
python -m isort -rc --skip venv .
python -m autopep8 -r --in-place --exclude venv .
