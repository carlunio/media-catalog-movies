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


DEFAULT_TIMEOUT_SECONDS = _as_float(os.getenv("API_TIMEOUT_SECONDS"), 180.0)
LONG_TIMEOUT_SECONDS = _as_float(os.getenv("API_LONG_TIMEOUT_SECONDS"), 900.0)
TIMEOUT_MODE_NORMAL = "normal"
TIMEOUT_MODE_UNITARY = "unitary"
TIMEOUT_MODE_DISABLED = "disabled"
TIMEOUT_MODE_OPTIONS = (
    TIMEOUT_MODE_NORMAL,
    TIMEOUT_MODE_UNITARY,
    TIMEOUT_MODE_DISABLED,
)
TIMEOUT_MODE_SESSION_KEY = "api_timeout_mode"
TIMEOUT_UNITARY_SESSION_KEY = "api_timeout_unitary_seconds"


def configure_page() -> None:
    try:
        st.set_page_config(page_title="Media Catalog Movies", layout="wide")
    except Exception:
        # Ignore repeated calls when Streamlit has already configured the page.
        pass


def _url(path: str) -> str:
    return f"{API_URL}{path}"


def _get_session_value(key: str, default: Any) -> Any:
    try:
        return st.session_state.get(key, default)
    except Exception:
        return default


def _set_session_value(key: str, value: Any) -> None:
    try:
        st.session_state[key] = value
    except Exception:
        pass


def _normalize_timeout_mode(raw: Any) -> str:
    mode = str(raw or "").strip().lower()
    return mode if mode in TIMEOUT_MODE_OPTIONS else TIMEOUT_MODE_DISABLED


def _unitary_timeout_seconds() -> float:
    raw = _get_session_value(TIMEOUT_UNITARY_SESSION_KEY, DEFAULT_TIMEOUT_SECONDS)
    try:
        value = float(raw)
    except (TypeError, ValueError):
        value = DEFAULT_TIMEOUT_SECONDS
    return value if value > 0 else DEFAULT_TIMEOUT_SECONDS


def _effective_timeout(timeout: float | None) -> float | None:
    base_timeout = DEFAULT_TIMEOUT_SECONDS if timeout is None else timeout
    mode = _normalize_timeout_mode(
        _get_session_value(TIMEOUT_MODE_SESSION_KEY, TIMEOUT_MODE_NORMAL)
    )
    if mode == TIMEOUT_MODE_DISABLED:
        return None
    if mode == TIMEOUT_MODE_UNITARY:
        return _unitary_timeout_seconds()
    return base_timeout


def render_timeout_controls() -> None:
    current_mode = _normalize_timeout_mode(
        _get_session_value(TIMEOUT_MODE_SESSION_KEY, TIMEOUT_MODE_DISABLED)
    )
    current_unitary = _unitary_timeout_seconds()

    with st.sidebar.expander("HTTP timeout", expanded=False):
        label_to_mode = {
            "Normal (por tipo de llamada)": TIMEOUT_MODE_NORMAL,
            "Unitario (un solo timeout)": TIMEOUT_MODE_UNITARY,
            "Desactivado (sin timeout)": TIMEOUT_MODE_DISABLED,
        }
        labels = list(label_to_mode.keys())
        selected_label = labels[list(label_to_mode.values()).index(current_mode)]
        mode_label = st.selectbox("Modo", labels, index=labels.index(selected_label))
        mode = label_to_mode[mode_label]
        _set_session_value(TIMEOUT_MODE_SESSION_KEY, mode)

        unitary_value = st.number_input(
            "Timeout unitario (segundos)",
            min_value=1.0,
            max_value=7200.0,
            value=float(current_unitary),
            step=1.0,
            disabled=(mode != TIMEOUT_MODE_UNITARY),
        )
        _set_session_value(TIMEOUT_UNITARY_SESSION_KEY, float(unitary_value))

        if mode == TIMEOUT_MODE_NORMAL:
            st.caption(
                f"Normal activo. API_TIMEOUT_SECONDS={DEFAULT_TIMEOUT_SECONDS:g}s, "
                f"API_LONG_TIMEOUT_SECONDS={LONG_TIMEOUT_SECONDS:g}s."
            )
        elif mode == TIMEOUT_MODE_UNITARY:
            st.caption(
                f"Unitario activo: {float(unitary_value):g}s para TODAS las llamadas."
            )
        else:
            st.caption("Timeout desactivado: requests espera indefinidamente.")


def api_get(path: str, *, timeout: float | None = None, **kwargs) -> Any:
    resolved_timeout = _effective_timeout(timeout)
    response = requests.get(_url(path), timeout=resolved_timeout, **kwargs)
    response.raise_for_status()
    return response.json()


def api_post(path: str, *, timeout: float | None = None, **kwargs) -> Any:
    resolved_timeout = _effective_timeout(timeout)
    response = requests.post(_url(path), timeout=resolved_timeout, **kwargs)
    response.raise_for_status()
    return response.json()


def api_put(path: str, *, timeout: float | None = None, **kwargs) -> Any:
    resolved_timeout = _effective_timeout(timeout)
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
            "needs_workflow_review": 0,
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
