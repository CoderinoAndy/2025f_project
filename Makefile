PYTHON ?= python
APP_DIR := 2025f_project
REQUIREMENTS_FILE := $(APP_DIR)/requirements.txt

ifeq ($(OS),Windows_NT)
VENV_PY := $(abspath venv/Scripts/python.exe)
else
VENV_PY := $(abspath venv/bin/python)
endif

.PHONY: help venv install sync run smoke

help:
	@echo "Targets:"
	@echo "  make venv     - Create venv/ if missing"
	@echo "  make install  - Install/upgrade pip + requirements into venv/"
	@echo "  make sync     - Alias for install"
	@echo "  make run      - Run Flask app with venv interpreter"
	@echo "  make smoke    - Quick import/startup check in venv"

venv:
	"$(PYTHON)" -m venv venv

install: venv
	"$(VENV_PY)" -m pip install -r "$(REQUIREMENTS_FILE)"

sync: install

run: install
	cd "$(APP_DIR)" && "$(VENV_PY)" run.py

smoke: install
	cd "$(APP_DIR)" && "$(VENV_PY)" -c "from app import create_app; create_app(); print('smoke_ok')"
