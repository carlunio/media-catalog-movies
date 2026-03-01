import os
from typing import Any

import requests
import streamlit as st

API_URL = os.getenv("API_URL", "http://127.0.0.1:8000")
WORKFLOW_STAGES = ("extraction", "imdb", "title_es", "omdb", "translation")
STAGE_ALIASES = {
    "extract_title_team": "extraction",
    "search_imdb": "imdb",
    "fetch_imdb_title_es": "title_es",
    "fetch_omdb": "omdb",
    "translate_plot": "translation",
}
GLOBAL_SELECTED_MOVIE_KEY = "global_selected_movie_id"
GLOBAL_SELECTED_MOVIE_SEQ_KEY = "global_selected_movie_seq"


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
THEME_APPLIED_KEY = "_ui_theme_applied"

THEME_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;700&display=swap');
@import url('https://fonts.googleapis.com/css2?family=Fraunces:opsz,wght@9..144,700&display=swap');

:root {
  --app-bg: #f1f6f8;
  --panel-bg: rgba(255, 255, 255, 0.88);
  --panel-border: rgba(0, 95, 115, 0.15);
  --text-main: #153038;
  --text-soft: #3a5f67;
  --brand: #005f73;
  --brand-2: #0a9396;
  --accent: #ee9b00;
  --danger: #bb3e03;
}

html, body, [class*="css"] {
  font-family: "Space Grotesk", "Segoe UI", sans-serif;
}

.stApp {
  color: var(--text-main);
  background:
    radial-gradient(circle at 8% 2%, rgba(10, 147, 150, 0.15), transparent 26%),
    radial-gradient(circle at 98% 2%, rgba(238, 155, 0, 0.14), transparent 24%),
    linear-gradient(180deg, #f5fafb 0%, var(--app-bg) 100%);
}

[data-testid="stHeader"] {
  background: transparent;
}

[data-testid="stSidebar"] > div:first-child {
  background: linear-gradient(180deg, rgba(0, 95, 115, 0.08), rgba(10, 147, 150, 0.03));
  border-right: 1px solid var(--panel-border);
}

h1, h2, h3 {
  color: #0a2e37;
  letter-spacing: -0.02em;
}

h1 {
  font-family: "Fraunces", serif;
  font-size: clamp(1.9rem, 2vw, 2.4rem);
}

[data-testid="stVerticalBlock"] > [data-testid="stElementContainer"] > div[data-testid="stMarkdownContainer"] p {
  color: var(--text-soft);
}

[data-testid="stMetric"] {
  background: linear-gradient(160deg, rgba(255,255,255,0.92), rgba(240, 250, 250, 0.82));
  border: 1px solid var(--panel-border);
  border-radius: 14px;
  padding: 0.45rem 0.65rem;
  box-shadow: 0 10px 24px -18px rgba(0, 67, 89, 0.55);
}

[data-testid="stButton"] > button {
  border-radius: 10px;
  border: 1px solid rgba(0, 95, 115, 0.28);
  background: linear-gradient(180deg, #12758c, var(--brand));
  color: #f5fcff;
  font-weight: 600;
  box-shadow: 0 10px 18px -14px rgba(0, 95, 115, 0.95);
}

[data-testid="stButton"] > button:hover {
  transform: translateY(-1px);
  border-color: rgba(0, 95, 115, 0.5);
  background: linear-gradient(180deg, #14859f, #007287);
}

[data-testid="stButton"] > button:disabled {
  opacity: 0.42;
}

[data-baseweb="input"] > div,
div[data-baseweb="select"] > div,
div[data-baseweb="textarea"] > div {
  border-radius: 10px;
  border-color: rgba(21, 48, 56, 0.22);
  background: rgba(255, 255, 255, 0.85);
}

[data-baseweb="input"] > div:focus-within,
div[data-baseweb="select"] > div:focus-within,
div[data-baseweb="textarea"] > div:focus-within {
  border-color: var(--brand-2);
  box-shadow: 0 0 0 0.18rem rgba(10, 147, 150, 0.18);
}

[data-testid="stDataFrame"],
[data-testid="stTable"] {
  background: var(--panel-bg);
  border: 1px solid var(--panel-border);
  border-radius: 14px;
  overflow: hidden;
}

[data-testid="stAlert"] {
  border-radius: 12px;
  border: 1px solid rgba(0, 95, 115, 0.14);
}

code {
  color: #0f4961 !important;
  background: rgba(15, 105, 135, 0.08) !important;
  border-radius: 6px;
  padding: 0.08rem 0.35rem;
}

.st-emotion-cache-13ln4jf {
  max-width: 1220px;
}
</style>
"""


def _apply_theme() -> None:
    applied = _get_session_value(THEME_APPLIED_KEY, False)
    if not applied:
        st.markdown(THEME_CSS, unsafe_allow_html=True)
        _set_session_value(THEME_APPLIED_KEY, True)
    else:
        # Keep styles present after reruns or page switches.
        st.markdown(THEME_CSS, unsafe_allow_html=True)


def configure_page() -> None:
    try:
        st.set_page_config(page_title="Media Catalog Movies", layout="wide")
    except Exception:
        # Ignore repeated calls when Streamlit has already configured the page.
        pass
    _apply_theme()


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

    with st.sidebar.expander("Debug UI state", expanded=False):
        if st.button("Resetear estado de UI", width="stretch"):
            try:
                st.session_state.clear()
            except Exception:
                pass
            try:
                st.cache_data.clear()
            except Exception:
                pass
            st.rerun()


def get_selected_movie_id() -> str | None:
    value = _get_session_value(GLOBAL_SELECTED_MOVIE_KEY, None)
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _get_selected_movie_seq() -> int:
    raw = _get_session_value(GLOBAL_SELECTED_MOVIE_SEQ_KEY, 0)
    try:
        return int(raw)
    except (TypeError, ValueError):
        return 0


def set_selected_movie_id(movie_id: str | None) -> None:
    text = str(movie_id or "").strip()
    if not text:
        return

    current = get_selected_movie_id()
    if current == text:
        return

    _set_session_value(GLOBAL_SELECTED_MOVIE_KEY, text)
    _set_session_value(GLOBAL_SELECTED_MOVIE_SEQ_KEY, _get_selected_movie_seq() + 1)


def normalize_workflow_stage(value: str | None) -> str | None:
    if not value:
        return None

    text = value.strip().lower()
    if not text:
        return None

    normalized = STAGE_ALIASES.get(text, text)
    if normalized in WORKFLOW_STAGES:
        return normalized

    if ":" in text:
        prefix = text.split(":", 1)[0].strip()
        prefixed = STAGE_ALIASES.get(prefix, prefix)
        if prefixed in WORKFLOW_STAGES:
            return prefixed

    for stage in WORKFLOW_STAGES:
        if stage in text:
            return stage

    return None


def infer_review_stage(movie: dict[str, Any]) -> str | None:
    for raw in (movie.get("workflow_current_node"), movie.get("workflow_review_reason")):
        stage = normalize_workflow_stage(str(raw) if raw is not None else None)
        if stage:
            return stage
    return None


def build_review_rerun_options(review_stage: str) -> list[tuple[str, str]]:
    idx = WORKFLOW_STAGES.index(review_stage)
    options: list[tuple[str, str]] = [(f"Reejecutar fase {review_stage}", review_stage)]
    for start_stage in reversed(WORKFLOW_STAGES[:idx]):
        options.append((f"Ejecutar desde {start_stage} hasta {review_stage}", start_stage))
    return options


def movie_selector_label(movie: dict[str, Any]) -> str:
    movie_id = str(movie.get("id") or "").strip() or "?"
    title = (
        str(movie.get("manual_title") or "").strip()
        or str(movie.get("extraction_title") or "").strip()
        or "(sin titulo)"
    )
    stage = str(movie.get("pipeline_stage") or "unknown")
    review = " | review" if bool(movie.get("workflow_needs_review")) else ""
    return f"{movie_id} | {title} | {stage}{review}"


def select_movie_id(
    rows: list[dict[str, Any]],
    *,
    label: str,
    key: str,
) -> str:
    if not rows:
        raise ValueError("rows cannot be empty")

    movie_ids = [str(row.get("id") or "").strip() for row in rows]
    movie_ids = [movie_id for movie_id in movie_ids if movie_id]
    if not movie_ids:
        raise ValueError("rows do not contain valid movie ids")

    labels = {
        str(row.get("id") or "").strip(): movie_selector_label(row)
        for row in rows
        if str(row.get("id") or "").strip()
    }

    preferred = get_selected_movie_id()
    if preferred not in movie_ids:
        preferred = movie_ids[0]

    current_widget_value = _get_session_value(key, None)
    seen_seq_key = f"{key}__seen_global_seq"
    seen_seq_raw = _get_session_value(seen_seq_key, -1)
    try:
        seen_seq = int(seen_seq_raw)
    except (TypeError, ValueError):
        seen_seq = -1

    global_seq = _get_selected_movie_seq()
    should_apply_global = seen_seq != global_seq or current_widget_value not in movie_ids
    if should_apply_global:
        _set_session_value(key, preferred)
        _set_session_value(seen_seq_key, global_seq)

    selected = st.selectbox(
        label,
        movie_ids,
        key=key,
        format_func=lambda movie_id: labels.get(movie_id, movie_id),
    )
    set_selected_movie_id(selected)
    _set_session_value(seen_seq_key, _get_selected_movie_seq())
    return selected


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
            "needs_title_es": 0,
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
