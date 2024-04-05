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
	sudo ${PYTHON} pg_logidater/cli.py --database bitbucket --master-host 10.123.9.11 --replica-host 10.123.9.12 --psql-user repmgr --repl-name make_test $(args)

run-conf:
	sudo ${PYTHON} pg_logidater/cli.py --saved-conf pg_logidater.conf $(args)

run-verbose:
	sudo ${PYTHON} pg_logidater/cli.py --verbose --database bitbucket --master-host 10.123.9.11 --replica-host 10.123.9.12 --psql-user repmgr --repl-name make_test $(args)

run-debug:
	sudo ${PYTHON} pg_logidater/cli.py --debug --database bitbucket --master-host 10.123.9.11 --replica-host 10.123.9.12 --psql-user repmgr --repl-name make_test $(args)
