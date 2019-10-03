# TODO: Possibly make sure that the environment is based on the requirements* files
# conda install -y -q --prefix venv -c conda-forge --file requirements.txt
# conda install -y -q --prefix venv -c conda-forge --file requirements_dev.txt
# conda install -y -q --prefix venv -c conda-forge --file requirements_test.txt
# eval "$(conda shell.bash hook)"
# conda activate ./venv
# pip install -r requirements_dev_pip.txt
# pip install -r requirements_test_pip.txt

conda env export -p venv | grep -v 'prefix: ' | grep -v 'name: ' > environment.yml
