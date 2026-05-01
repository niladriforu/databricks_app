# Requires: pip install pre-commit  (or poetry run …)
.PHONY: help install-hooks check fix

help:
	@echo "make install-hooks  — run pre-commit install (wire Git to run hooks on commit)"
	@echo "make check          — run all pre-commit hooks on every file"
	@echo "make fix            — same as check (ruff --fix + format via hooks)"

install-hooks:
	pre-commit install

check:
	pre-commit run --all-files

fix: check
