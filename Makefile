# Requires: pip install pre-commit
# Optional: pip install ruff isort black in ./venv (or poetry run …)
PYTHON ?= python3
PYTHON_SRC ?= src
VENV_BIN := $(CURDIR)/venv/bin
# Prefer project venv, then PATH, then python -m (needs package on that interpreter)
RUFF ?= $(or $(wildcard $(VENV_BIN)/ruff),$(shell command -v ruff 2>/dev/null),$(PYTHON) -m ruff)
ISORT ?= $(or $(wildcard $(VENV_BIN)/isort),$(shell command -v isort 2>/dev/null),$(PYTHON) -m isort)
BLACK ?= $(or $(wildcard $(VENV_BIN)/black),$(shell command -v black 2>/dev/null),$(PYTHON) -m black)

.PHONY: help install-hooks check fix lint ruff-fix format isort black fmt

help:
	@echo "Pre-commit (recommended — matches CI hooks):"
	@echo "  make install-hooks  — pre-commit install"
	@echo "  make check          — pre-commit run --all-files"
	@echo "  make fix            — same as check"
	@echo ""
	@echo "Run tools directly on $(PYTHON_SRC)/ (uses ./venv/bin/* if present, else PATH, else python -m):"
	@echo "  make lint           — ruff check"
	@echo "  make ruff-fix       — ruff check --fix"
	@echo "  make format         — ruff format"
	@echo "  make isort          — isort (needs: pip install isort)"
	@echo "  make black          — black (needs: pip install black)"
	@echo "  make fmt            — ruff format + ruff --fix (usual one-shot)"
	@echo ""
	@echo "Note: pyproject already enables ruff rule I (import sort) and ruff-format."
	@echo "      isort/black are optional; mixing black + ruff format can disagree — pick one formatter."

install-hooks:
	pre-commit install

check:
	pre-commit run --all-files

fix: check

lint:
	$(RUFF) check $(PYTHON_SRC)

ruff-fix:
	$(RUFF) check --fix $(PYTHON_SRC)

format:
	$(RUFF) format $(PYTHON_SRC)

isort:
	$(ISORT) $(PYTHON_SRC)

black:
	$(BLACK) $(PYTHON_SRC)

fmt: format ruff-fix
