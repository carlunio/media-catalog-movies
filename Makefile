# =========================
# ENV
# =========================
include .env
export

# =========================
# PYTHON / VENV
# =========================
VENV := .venv
PYTHON := $(VENV)/bin/python
PIP := $(VENV)/bin/pip

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
	python3 -m venv $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) install -e .

install:
	$(PIP) install -e .

dev-back:
	@echo "Starting backend on port $(BACK_PORT)"
	@$(MAKE) stop-back
	$(VENV)/bin/uvicorn src.backend.main:app --reload --port $(BACK_PORT)

dev-front:
	@echo "Starting frontend on port $(FRONT_PORT)"
	@$(MAKE) stop-front
	$(VENV)/bin/streamlit run src/frontend/app.py

dev:
	make -j 2 dev-back dev-front

stop-back:
	@echo "Stopping backend (port $(BACK_PORT))"
	@lsof -ti :$(BACK_PORT) | xargs -r kill || true

stop-front:
	@echo "Stopping frontend (port $(FRONT_PORT))"
	@lsof -ti :$(FRONT_PORT) | xargs -r kill || true

stop: stop-back stop-front

restart:
	make stop
	make dev

clean:
	rm -rf $(VENV)
