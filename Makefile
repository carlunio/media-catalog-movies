# =========================
# ENV
# =========================
# Resolve everything relative to this Makefile so commands work even if
# `make -f ...` is executed from another directory.
MAKEFILE_DIR := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))
include $(MAKEFILE_DIR)/.env
export

# =========================
# PYTHON / VENV
# =========================
VENV := $(MAKEFILE_DIR)/.venv

ifeq ($(OS),Windows_NT)
PYTHON_BOOTSTRAP := py
VENV_BIN := $(VENV)/Scripts
PYTHON := $(VENV_BIN)/python.exe
PIP := $(VENV_BIN)/pip.exe
UVICORN := $(VENV_BIN)/uvicorn.exe
STREAMLIT := $(VENV_BIN)/streamlit.exe
RM_VENV := powershell -NoProfile -Command "if (Test-Path '$(VENV)') { Remove-Item -Recurse -Force '$(VENV)' }"
STOP_PORT = powershell -NoProfile -Command '$$pids = Get-NetTCPConnection -LocalPort $(1) -State Listen -ErrorAction SilentlyContinue | Select-Object -ExpandProperty OwningProcess -Unique; if ($$pids) { $$pids | ForEach-Object { Stop-Process -Id $$PSItem -Force -ErrorAction SilentlyContinue } }; exit 0'
STOP_BACK = powershell -NoProfile -Command '$$cmd = Get-CimInstance Win32_Process -ErrorAction SilentlyContinue | Where-Object { ($$_.Name -match "python|uvicorn") -and ($$_.CommandLine -match "src\\.backend\\.main:app") } | Select-Object -ExpandProperty ProcessId -Unique; if ($$cmd) { $$cmd | ForEach-Object { cmd /c "taskkill /PID $$_ /T /F >NUL 2>&1" } }; $$listen = Get-NetTCPConnection -LocalPort $(BACK_PORT) -State Listen -ErrorAction SilentlyContinue | Select-Object -ExpandProperty OwningProcess -Unique; if ($$listen) { $$listen | ForEach-Object { cmd /c "taskkill /PID $$_ /T /F >NUL 2>&1" } }; exit 0'
STOP_FRONT = powershell -NoProfile -Command '$$cmd = Get-CimInstance Win32_Process -ErrorAction SilentlyContinue | Where-Object { ($$_.Name -match "python|streamlit") -and ($$_.CommandLine -match "src/frontend/app.py") } | Select-Object -ExpandProperty ProcessId -Unique; if ($$cmd) { $$cmd | ForEach-Object { cmd /c "taskkill /PID $$_ /T /F >NUL 2>&1" } }; $$listen = Get-NetTCPConnection -LocalPort $(FRONT_PORT) -State Listen -ErrorAction SilentlyContinue | Select-Object -ExpandProperty OwningProcess -Unique; if ($$listen) { $$listen | ForEach-Object { cmd /c "taskkill /PID $$_ /T /F >NUL 2>&1" } }; exit 0'
else
PYTHON_BOOTSTRAP := python3
VENV_BIN := $(VENV)/bin
PYTHON := $(VENV_BIN)/python
PIP := $(VENV_BIN)/pip
UVICORN := $(VENV_BIN)/uvicorn
STREAMLIT := $(VENV_BIN)/streamlit
RM_VENV := rm -rf $(VENV)
STOP_PORT = lsof -ti :$(1) | xargs -r kill || true
# On Linux, avoid pkill patterns here because they can match the shell
# command spawned by make itself and terminate `make stop-back`.
STOP_BACK = lsof -ti :$(BACK_PORT) | xargs -r kill || true
STOP_FRONT = lsof -ti :$(FRONT_PORT) | xargs -r kill || true
endif

BACKEND_APP := src.backend.main:app
FRONTEND_APP := $(MAKEFILE_DIR)/src/frontend/app.py

# =========================
# PORTS
# =========================
BACK_PORT ?= 8000
FRONT_PORT ?= 8501

# =========================
# PHONY
# =========================
.PHONY: setup install dev-back dev-front dev stop stop-back stop-front restart clean

setup:
	$(PYTHON_BOOTSTRAP) -m venv $(VENV)
	$(PYTHON) -m pip install --upgrade pip
	$(PYTHON) -m pip install -e .

install:
	$(PYTHON) -m pip install -e .

dev-back:
	@echo "Starting backend on port $(BACK_PORT)"
	@$(MAKE) stop-back
	$(UVICORN) $(BACKEND_APP) --reload --port $(BACK_PORT)

dev-front:
	@echo "Starting frontend on port $(FRONT_PORT)"
	@$(MAKE) stop-front
	$(STREAMLIT) run $(FRONTEND_APP) --server.port $(FRONT_PORT)

dev:
	$(MAKE) -j 2 dev-back dev-front

stop-back:
	@echo "Stopping backend (port $(BACK_PORT))"
	@$(STOP_BACK)

stop-front:
	@echo "Stopping frontend (port $(FRONT_PORT))"
	@$(STOP_FRONT)

stop: stop-back stop-front

restart:
	$(MAKE) stop
	$(MAKE) dev

clean:
	$(RM_VENV)
