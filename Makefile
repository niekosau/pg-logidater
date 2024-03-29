default: venv
# Variables
VENV           = .venv
VENV_PYTHON    = $(VENV)/bin/python
SYSTEM_PYTHON  = $(or $(shell which python3.9), $(shell which python3), $(shell which python))
PYTHON         = $(or $(wildcard $(VENV_PYTHON)), $(SYSTEM_PYTHON))
venv:
	poetry env use python3.9

dev:
	$(PYTHON) setup.py develop

cleanup:
	rm -rf build/ dist/

flake8:
	$(PYTHON) -m flake8 pg-logidater

pytest:
	$(PYTHON) -m pytest -s -v

.PHONY: build buil-clean

run:
	poetry install
	${PYTHON} pg_logidater/cli.py $(args)

build_wheel:
	${PYTHON} setup.py bdist_wheel
