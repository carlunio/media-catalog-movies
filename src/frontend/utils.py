import html
import os
import time
from typing import Any

import requests
import streamlit as st

try:
    from src.project_meta import get_app_meta
except ModuleNotFoundError:  # pragma: no cover
    from project_meta import get_app_meta

API_URL = os.getenv("API_URL", "http://127.0.0.1:8000")
APP_META = get_app_meta()
PAGE_ICON = """
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 640 640">
<path d="M64 128C64 92.7 92.7 64 128 64H512C547.3 64 576 92.7 576 128V384C576 419.3 547.3 448 512 448H397.3L350.6 541.4C345.2 552.3 334 559.1 321.9 559.1C309.8 559.1 298.6 552.3 293.2 541.4L246.7 448H128C92.7 448 64 419.3 64 384V128zM192 176C174.3 176 160 190.3 160 208V304C160 321.7 174.3 336 192 336H448C465.7 336 480 321.7 480 304V208C480 190.3 465.7 176 448 176H192z"/>
</svg>
"""
WORKFLOW_STAGES = ("extraction", "imdb", "title_es", "omdb", "translation")
STAGE_ALIASES = {
    "extract_title_team": "extraction",
    "search_imdb": "imdb",
    "fetch_imdb_title_es": "title_es",
    "fetch_omdb": "omdb",
    "translate_plot": "translation",
}
STAGE_UI_LABELS = {
    "extraction": "Extracción",
    "imdb": "IMDb",
    "title_es": "IMDb título ES",
    "omdb": "OMDb",
    "translation": "Traducción",
    "review": "Revisión",
    "done": "Done",
    "running": "Running",
    "unknown": "Unknown",
}
NODE_UI_LABELS = {
    "extract_title_team": "extract_title_team",
    "search_imdb": "search_imdb",
    "fetch_imdb_title_es": "fetch_imdb_titles",
    "fetch_omdb": "fetch_omdb",
    "translate_plot": "translate_plot",
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
@import url('https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css');
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

section.main > div.block-container {
  max-width: 1360px;
  padding-top: 0.9rem;
  padding-bottom: 1.1rem;
}

[data-testid="stHeader"] {
  background: transparent;
}


.mc-heading {
  display: flex;
  align-items: center;
  gap: 0.62rem;
  color: var(--text-main);
  letter-spacing: 0;
  font-family: "Fraunces", "Space Grotesk", serif;
  font-weight: 700;
  line-height: 1.2;
}

.mc-heading i {
  color: var(--accent);
  min-width: 1.05em;
  text-align: center;
}

.mc-heading-1 {
  font-size: 2.25rem;
  margin: 0 0 0.3rem 0;
}

.mc-heading-2 {
  font-size: 1.28rem;
  margin: 0.35rem 0 0.25rem 0;
}

.mc-heading-3 {
  font-size: 1.08rem;
  margin: 0.25rem 0 0.2rem 0;
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
  padding-top: 0.35rem;
  padding-bottom: 0.35rem;
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

[data-testid="stVerticalBlock"] {
  gap: 0.45rem;
}

div[data-testid="stForm"] {
  border: 1px solid var(--panel-border);
  border-radius: 12px;
  background: rgba(255, 255, 255, 0.8);
  padding: 0.4rem 0.55rem;
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
        st.set_page_config(
            page_title=APP_META.app_name, page_icon=PAGE_ICON, layout="wide"
        )
    except Exception:
        # Ignore repeated calls when Streamlit has already configured the page.
        pass
    _apply_theme()
    st.sidebar.caption(f"Versión: {APP_META.display_version}")
    if APP_META.changelog_path.exists():
        st.sidebar.caption(f"Changelog: {APP_META.changelog_path.name}")


def render_icon_heading(
    text: str,
    *,
    icon: str,
    level: int = 1,
) -> None:
    safe_text = html.escape(text)
    safe_icon = html.escape(icon)
    level = max(1, min(level, 6))
    st.markdown(
        (
            f'<h{level} class="mc-heading mc-heading-{level}">'
            f'<i class="fa-solid fa-{safe_icon}"></i>'
            f"<span>{safe_text}</span>"
            f"</h{level}>"
        ),
        unsafe_allow_html=True,
    )


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
        if st.button("Restablecer estado de UI", width="stretch"):
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


def stage_ui_label(value: str | None) -> str:
    raw = str(value or "").strip()
    if not raw:
        return STAGE_UI_LABELS["unknown"]

    normalized = normalize_workflow_stage(raw)
    if raw.lower().startswith("running:"):
        suffix = raw.split(":", 1)[1].strip()
        node_label = node_ui_label(suffix)
        return (
            f"{STAGE_UI_LABELS['running']}:{node_label}"
            if node_label
            else STAGE_UI_LABELS["running"]
        )

    if normalized:
        return STAGE_UI_LABELS.get(normalized, normalized)
    return STAGE_UI_LABELS.get(raw.lower(), raw)


def node_ui_label(value: str | None) -> str:
    raw = str(value or "").strip()
    if not raw:
        return "-"
    return NODE_UI_LABELS.get(raw, raw)


def infer_review_stage(movie: dict[str, Any]) -> str | None:
    for raw in (
        movie.get("workflow_current_node"),
        movie.get("workflow_review_reason"),
    ):
        stage = normalize_workflow_stage(str(raw) if raw is not None else None)
        if stage:
            return stage
    return None


def build_review_rerun_options(review_stage: str) -> list[tuple[str, str]]:
    idx = WORKFLOW_STAGES.index(review_stage)
    target_label = stage_ui_label(review_stage)
    options: list[tuple[str, str]] = [(f"Reejecutar fase {target_label}", review_stage)]
    for start_stage in reversed(WORKFLOW_STAGES[:idx]):
        start_label = stage_ui_label(start_stage)
        options.append(
            (f"Ejecutar desde {start_label} hasta {target_label}", start_stage)
        )
    return options


def movie_selector_label(movie: dict[str, Any]) -> str:
    movie_id = str(movie.get("id") or "").strip() or "?"
    title = (
        str(movie.get("manual_title") or "").strip()
        or str(movie.get("extraction_title") or "").strip()
        or "(sin título)"
    )
    stage = stage_ui_label(str(movie.get("pipeline_stage") or "unknown"))
    review = " | revisión" if bool(movie.get("workflow_needs_review")) else ""
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

    preferred_global = get_selected_movie_id()
    current_widget_value = _get_session_value(key, None)
    seen_seq_key = f"{key}__seen_global_seq"
    seen_seq_raw = _get_session_value(seen_seq_key, -1)
    try:
        seen_seq = int(seen_seq_raw)
    except (TypeError, ValueError):
        seen_seq = -1

    global_seq = _get_selected_movie_seq()
    global_changed = seen_seq != global_seq
    if global_changed:
        if preferred_global in movie_ids:
            _set_session_value(key, preferred_global)
        elif current_widget_value in movie_ids:
            _set_session_value(key, current_widget_value)
        else:
            _set_session_value(key, movie_ids[0])
        _set_session_value(seen_seq_key, global_seq)
    elif current_widget_value not in movie_ids:
        if preferred_global in movie_ids:
            _set_session_value(key, preferred_global)
        else:
            _set_session_value(key, movie_ids[0])

    selected = st.selectbox(
        label,
        movie_ids,
        key=key,
        format_func=lambda movie_id: labels.get(movie_id, movie_id),
    )
    set_selected_movie_id(selected)
    _set_session_value(seen_seq_key, _get_selected_movie_seq())
    return selected


def render_movie_prev_next(
    rows: list[dict[str, Any]],
    selected_id: str,
    *,
    key_prefix: str,
    noun: str = "Película",
) -> None:
    movie_ids = [
        str(row.get("id") or "").strip()
        for row in rows
        if str(row.get("id") or "").strip()
    ]
    if selected_id not in movie_ids:
        return

    current_index = movie_ids.index(selected_id)
    nav_left, nav_center, nav_right = st.columns([1, 2, 1], gap="small")
    with nav_left:
        if st.button(
            "Anterior",
            disabled=current_index == 0,
            key=f"{key_prefix}_prev",
            width="stretch",
        ):
            set_selected_movie_id(movie_ids[current_index - 1])
            st.rerun()
    with nav_center:
        st.caption(f"{noun} {current_index + 1} de {len(movie_ids)} en este filtro.")
    with nav_right:
        if st.button(
            "Siguiente",
            disabled=current_index >= len(movie_ids) - 1,
            key=f"{key_prefix}_next",
            width="stretch",
        ):
            set_selected_movie_id(movie_ids[current_index + 1])
            st.rerun()


def api_get(path: str, *, timeout: float | None = None, **kwargs) -> Any:
    resolved_timeout = _effective_timeout(timeout)
    response = requests.get(_url(path), timeout=resolved_timeout, **kwargs)
    response.raise_for_status()
    return response.json()


def api_get_bytes(path: str, *, timeout: float | None = None, **kwargs) -> bytes:
    resolved_timeout = _effective_timeout(timeout)
    response = requests.get(_url(path), timeout=resolved_timeout, **kwargs)
    response.raise_for_status()
    return response.content


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
    last_exc: Exception | None = None
    max_attempts = 20
    for attempt in range(max_attempts):
        try:
            api_get("/health", timeout=3.0)
            st.success(f"Backend disponible: {API_URL}")
            return
        except Exception as exc:
            last_exc = exc
            if attempt < (max_attempts - 1):
                time.sleep(0.5)

    st.error(f"Backend no disponible: {API_URL} ({last_exc})")


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


@st.cache_data(ttl=60)
def load_cover_name_audit() -> dict[str, Any]:
    payload = api_get("/covers/name-audit")
    return payload if isinstance(payload, dict) else {}


@st.cache_data(ttl=30)
def load_ollama_models() -> list[str]:
    payload = api_get("/models/ollama")
    raw = payload.get("models", []) if isinstance(payload, dict) else []
    return [str(item).strip() for item in raw if str(item).strip()]


def select_ollama_model(
    label: str,
    default_model: str,
    *,
    key: str,
) -> str:
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
