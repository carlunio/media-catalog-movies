import os

import requests
import streamlit as st

try:
    from src.frontend.utils import (
        LONG_TIMEOUT_SECONDS,
        api_get,
        api_post,
        api_put,
        build_review_rerun_options,
        configure_page,
        render_icon_heading,
        render_movie_prev_next,
        infer_review_stage,
        render_timeout_controls,
        select_movie_id,
        select_ollama_model,
    )
except ModuleNotFoundError:  # pragma: no cover
    from frontend.utils import (
        LONG_TIMEOUT_SECONDS,
        api_get,
        api_post,
        api_put,
        build_review_rerun_options,
        configure_page,
        render_icon_heading,
        render_movie_prev_next,
        infer_review_stage,
        render_timeout_controls,
        select_movie_id,
        select_ollama_model,
    )

configure_page()
render_icon_heading("Fase 5 - Traducción de sinopsis", icon="language", level=1)
render_timeout_controls()


def _switch_page(target: str) -> None:
    switch_fn = getattr(st, "switch_page", None)
    if callable(switch_fn):
        switch_fn(target)
    else:
        st.info("Tu versión de Streamlit no soporta `switch_page`.")


def _filter_rows(
    rows: list[dict],
    *,
    mode: str,
) -> list[dict]:
    if mode == "review":
        return [row for row in rows if row.get("workflow_needs_review")]
    if mode == "review_stage":
        return [
            row
            for row in rows
            if row.get("workflow_needs_review") and infer_review_stage(row) == "translation"
        ]
    return rows


render_icon_heading("Acciones por lote (opcional)", icon="list", level=2)
with st.expander("Traducir sinopsis por lote", expanded=False):
    batch_c1, batch_c2, batch_c3, batch_c4 = st.columns([2, 1, 1, 2])
    with batch_c1:
        movie_id = st.text_input("ID concreto (opcional)", value="")
    with batch_c2:
        limit = st.number_input("Límite batch", min_value=1, max_value=5000, value=20)
    with batch_c3:
        overwrite = st.checkbox("Retraducir", value=False)
    with batch_c4:
        model = select_ollama_model(
            "Modelo traducción",
            os.getenv("TRANSLATION_MODEL", "phi4:latest"),
            key="translation_model",
        )

    if st.button("Traducir sinopsis (batch)"):
        try:
            result = api_post(
                "/plot/translate",
                json={
                    "movie_id": movie_id or None,
                    "limit": int(limit),
                    "overwrite": overwrite,
                    "model": model,
                },
                timeout=LONG_TIMEOUT_SECONDS,
            )
            st.success("Traducción completada")
            st.json(result)
        except requests.exceptions.ReadTimeout:
            st.error(
                "Timeout esperando al backend. "
                "Reduce el límite del lote o cambia el modo en Sidebar > HTTP timeout."
            )
        except Exception as exc:
            st.error(str(exc))

st.divider()

rows = api_get("/movies", params={"limit": 5000})
rows_with_plot = [row for row in rows if row.get("omdb_plot_en")]

if not rows_with_plot:
    st.info("No hay sinopsis en inglés disponibles")
    st.stop()

mode_labels = {
    "all": "Mostrar todas",
    "review": "Solo en revisión",
    "review_stage": "Solo revisión de traducción",
}
if "plot_filter_mode" not in st.session_state:
    st.session_state["plot_filter_mode"] = "all"

filter_mode = st.segmented_control(
    "Filtro",
    options=list(mode_labels.keys()),
    default=st.session_state["plot_filter_mode"],
    format_func=lambda value: mode_labels.get(str(value), str(value)),
    key="plot_filter_mode",
    width="stretch",
)
if filter_mode is None:
    filter_mode = "all"

filtered_rows = _filter_rows(
    rows_with_plot,
    mode=str(filter_mode),
)
st.caption(
    f"Filtro actual: {mode_labels.get(str(filter_mode), 'Mostrar todas')} | "
    f"{len(filtered_rows)} de {len(rows_with_plot)} películas"
)

if not filtered_rows:
    st.info("No hay películas con los filtros actuales.")
    st.stop()

selected_id = select_movie_id(filtered_rows, label="Película", key="plot_movie_selector")
render_movie_prev_next(filtered_rows, selected_id, key_prefix="plot_movie")


nav_f2, nav_f3, nav_f4 = st.columns(3)
with nav_f2:
    if st.button("Ir a Fase 2 - Título", width="stretch", key="plot_to_f2"):
        _switch_page("pages/02_Título.py")
with nav_f3:
    if st.button("Ir a Fase 3 - IMDb", width="stretch", key="plot_to_f3"):
        _switch_page("pages/03_IMDb.py")
with nav_f4:
    if st.button("Ir a Fase 4 - OMDb", width="stretch", key="plot_to_f4"):
        _switch_page("pages/04_OMDb.py")

movie = api_get(f"/movies/{selected_id}")
review_stage = infer_review_stage(movie)

render_icon_heading("Acciones sobre película seleccionada", icon="film", level=2)
st.write("Estado de traducción:", movie.get("translation_status") or "")
if review_stage:
    st.caption(f"Origen de revisión detectado: `{review_stage}`")
if movie.get("translation_last_error"):
    st.warning(movie.get("translation_last_error"))
if movie.get("workflow_needs_review"):
    st.warning(movie.get("workflow_review_reason") or "Pendiente de revisión")

plot_c1, plot_c2 = st.columns(2)
with plot_c1:
    render_icon_heading("Sinopsis original (EN)", icon="file-lines", level=3)
    st.text_area(
        "EN",
        value=movie.get("omdb_plot_en") or "",
        height=240,
        disabled=True,
        key=f"plot_en_view_{selected_id}",
    )
    if st.button("Traducir solo este ID"):
        try:
            result = api_post(
                "/plot/translate",
                json={
                    "movie_id": selected_id,
                    "limit": 1,
                    "overwrite": True,
                    "model": model,
                },
                timeout=LONG_TIMEOUT_SECONDS,
            )
            st.success("Traducción completada")
            st.json(result)
        except requests.exceptions.ReadTimeout:
            st.error("Timeout esperando al backend para este ID. Prueba Sidebar > HTTP timeout.")
        except Exception as exc:
            st.error(str(exc))

with plot_c2:
    render_icon_heading("Sinopsis traducida (ES)", icon="language", level=3)
    plot_es = st.text_area(
        "ES",
        value=movie.get("omdb_plot_es") or "",
        height=240,
        key=f"plot_es_edit_{selected_id}",
    )
    if st.button("Guardar traducción manual"):
        try:
            api_put(f"/movies/{selected_id}/plot-es", json={"plot_es": plot_es})
            st.success("Traducción guardada")
        except Exception as exc:
            st.error(str(exc))

st.divider()
render_icon_heading("Reejecución acotada hasta revisión", icon="rotate-right", level=2)
if not movie.get("workflow_needs_review"):
    st.info("Esta película no está en revisión.")
else:
    stage_target = review_stage or "translation"
    options = build_review_rerun_options(stage_target)
    option_labels = [label for label, _ in options]
    option_map = {label: start for label, start in options}
    selected_option = st.selectbox("Reejecución disponible", option_labels, index=0, key="plot_rerun_option")
    selected_start_stage = option_map[selected_option]

    if st.button("Reejecutar workflow hasta revisión", key="plot_rerun_btn"):
        payload = {
            "movie_id": selected_id,
            "limit": 1,
            "start_stage": selected_start_stage,
            "stop_after": stage_target,
            "overwrite": True,
        }
        try:
            result = api_post("/workflow/run", json=payload, timeout=LONG_TIMEOUT_SECONDS)
            st.success(f"Workflow relanzado desde {selected_start_stage} hasta {stage_target}.")
            st.json(result)
        except requests.exceptions.ReadTimeout:
            st.error("Timeout relanzando workflow")
        except Exception as exc:
            st.error(str(exc))
