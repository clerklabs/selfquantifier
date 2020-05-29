## Development

### Setting up the development environment

```
./setup-python-venv.sh dev
```

### Run tests

```
conda activate ./venvdev
pytest -vv
```

### Add deps

Add the dep to `requirements.txt` or `requirements_dev.txt`
Then, clean the environment as per below, or run the possibly quicker:

```
conda activate ./venv
pip install -r requirements.txt
conda activate ./venvdev
pip install -r requirements.txt
pip install -r requirements_dev.txt
```

### Cleaning the environment

To get an environment solely dependent on what is installed via requirements.txt:
```
./setup-python-venv.sh
```

Similarly for the dev env:
```
./setup-python-venv.sh dev
```

### Releasing a new version

Bump version, tag, push and create a new release on Github, then:

```
python setup.py sdist
twine upload dist/clerkai-0.1.2.tar.gz # change to the version that is being released
```
