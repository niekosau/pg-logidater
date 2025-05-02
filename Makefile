default: run
# Variables
VENV           = .venv
VENV_PYTHON    = $(VENV)/bin/python
SYSTEM_PYTHON  = $(or $(shell which python3.9), $(shell which python3), $(shell which python))
PYTHON         = $(or $(wildcard $(VENV_PYTHON)), $(SYSTEM_PYTHON))

.PHONY: run
run:
	@echo "Running pg-logidater..."
	uv run sudo .venv/bin/pg-logidater --log-level debug --saved-conf dev-config.conf setup-replica $(ARGS)

.PHONY: sequence
sequence:
	@echo "Updating sequences..."
	uv run sudo .venv/bin/pg-logidater --log-level debug --saved-conf dev-config.conf sync-sequences $(ARGS)

.PHONY: cleanup
cleanup:
	@echo "Cleaning up..."
	uv run sudo .venv/bin/pg-logidater --saved-conf dev-config.conf drop-setup
