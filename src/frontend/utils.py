import os
from typing import Any

import requests
import streamlit as st

API_URL = os.getenv("API_URL", "http://127.0.0.1:8000")


def _as_float(raw: str | None, default: float) -> float:
    try:
        return float(raw) if raw is not None else default
    except ValueError:
        return default


DEFAULT_TIMEOUT_SECONDS = _as_float(os.getenv("API_TIMEOUT_SECONDS"), 30.0)
LONG_TIMEOUT_SECONDS = _as_float(os.getenv("API_LONG_TIMEOUT_SECONDS"), 900.0)


def _url(path: str) -> str:
    return f"{API_URL}{path}"


def api_get(path: str, *, timeout: float | None = None, **kwargs) -> Any:
    resolved_timeout = DEFAULT_TIMEOUT_SECONDS if timeout is None else timeout
    response = requests.get(_url(path), timeout=resolved_timeout, **kwargs)
    response.raise_for_status()
    return response.json()


def api_post(path: str, *, timeout: float | None = None, **kwargs) -> Any:
    resolved_timeout = DEFAULT_TIMEOUT_SECONDS if timeout is None else timeout
    response = requests.post(_url(path), timeout=resolved_timeout, **kwargs)
    response.raise_for_status()
    return response.json()


def api_put(path: str, *, timeout: float | None = None, **kwargs) -> Any:
    resolved_timeout = DEFAULT_TIMEOUT_SECONDS if timeout is None else timeout
    response = requests.put(_url(path), timeout=resolved_timeout, **kwargs)
    response.raise_for_status()
    return response.json()


def show_backend_status() -> None:
    try:
        api_get("/health")
        st.success(f"Backend reachable: {API_URL}")
    except Exception as exc:
        st.error(f"Backend not reachable: {API_URL} ({exc})")


def load_stats() -> dict[str, int]:
    try:
        return api_get("/stats")
    except Exception:
        return {
            "total": 0,
            "needs_extraction": 0,
            "needs_manual_review": 0,
            "needs_imdb": 0,
            "needs_omdb": 0,
            "needs_translation": 0,
        }


@st.cache_data(ttl=30)
def load_ollama_models() -> list[str]:
    payload = api_get("/models/ollama")
    raw = payload.get("models", []) if isinstance(payload, dict) else []
    return [str(item).strip() for item in raw if str(item).strip()]


def select_ollama_model(label: str, default_model: str, *, key: str) -> str:
    resolved_default = (default_model or "").strip()

    try:
        available = load_ollama_models()
    except Exception:
        available = []

    options = available[:]
    if resolved_default and resolved_default not in options:
        options.insert(0, resolved_default)
    if not options:
        options = [resolved_default] if resolved_default else [""]

    index = options.index(resolved_default) if resolved_default in options else 0
    return st.selectbox(label, options, index=index, key=key)
