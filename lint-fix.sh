set -x
python -m isort -rc --skip venv .
python -m autopep8 -r --in-place --exclude venv .
