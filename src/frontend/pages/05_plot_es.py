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
        infer_review_stage,
        render_timeout_controls,
        select_movie_id,
        select_ollama_model,
    )

configure_page()
st.title("Fase 5 - Traduccion plot")
render_timeout_controls()


def _switch_page(target: str) -> None:
    switch_fn = getattr(st, "switch_page", None)
    if callable(switch_fn):
        switch_fn(target)
    else:
        st.info("Tu version de Streamlit no soporta switch_page.")


def _filter_rows(
    rows: list[dict],
    *,
    review_only: bool,
    translation_review_only: bool,
) -> list[dict]:
    out = rows
    if review_only:
        out = [row for row in out if row.get("workflow_needs_review")]
    if translation_review_only:
        out = [
            row
            for row in out
            if row.get("workflow_needs_review") and infer_review_stage(row) == "translation"
        ]
    return out


movie_id = st.text_input("ID concreto (opcional)", value="")
limit = st.number_input("Limite batch", min_value=1, max_value=5000, value=20)
overwrite = st.checkbox("Retraducir aunque ya exista", value=False)
model = select_ollama_model(
    "Modelo traduccion",
    os.getenv("TRANSLATION_MODEL", "phi4:latest"),
    key="translation_model",
)

if st.button("Traducir plots"):
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
        st.success("Traduccion completada")
        st.json(result)
    except requests.exceptions.ReadTimeout:
        st.error(
            "Timeout esperando al backend. "
            "Reduce el limite batch o cambia el modo en Sidebar > HTTP timeout."
        )
    except Exception as exc:
        st.error(str(exc))

st.divider()

rows = api_get("/movies", params={"limit": 5000})
rows_with_plot = [row for row in rows if row.get("omdb_plot_en")]

if not rows_with_plot:
    st.info("No hay plots en ingles disponibles")
    st.stop()

if "plot_review_only_filter" not in st.session_state:
    st.session_state["plot_review_only_filter"] = False
if "plot_stage_review_only_filter" not in st.session_state:
    st.session_state["plot_stage_review_only_filter"] = False

f1, f2, _ = st.columns([1, 1, 4])
with f1:
    review_label = "Mostrar todas" if st.session_state["plot_review_only_filter"] else "Solo en review"
    if st.button(review_label, key="plot_toggle_review"):
        st.session_state["plot_review_only_filter"] = not st.session_state["plot_review_only_filter"]
        st.rerun()
with f2:
    stage_label = (
        "Mostrar review global"
        if st.session_state["plot_stage_review_only_filter"]
        else "Solo review de traduccion"
    )
    if st.button(stage_label, key="plot_toggle_stage_review"):
        st.session_state["plot_stage_review_only_filter"] = not st.session_state[
            "plot_stage_review_only_filter"
        ]
        st.rerun()

filtered_rows = _filter_rows(
    rows_with_plot,
    review_only=bool(st.session_state["plot_review_only_filter"]),
    translation_review_only=bool(st.session_state["plot_stage_review_only_filter"]),
)
st.caption(f"Filtro actual: {len(filtered_rows)} de {len(rows_with_plot)} peliculas")

if not filtered_rows:
    st.info("No hay peliculas con los filtros actuales.")
    st.stop()

selected_id = select_movie_id(filtered_rows, label="Pelicula", key="plot_movie_selector")

nav_f2, nav_f3, nav_f4 = st.columns(3)
with nav_f2:
    if st.button("Ir a Fase 2 - Revision", width="stretch", key="plot_to_f2"):
        _switch_page("pages/02_revision_titulo_equipo.py")
with nav_f3:
    if st.button("Ir a Fase 3 - IMDb", width="stretch", key="plot_to_f3"):
        _switch_page("pages/03_imdb.py")
with nav_f4:
    if st.button("Ir a Fase 4 - OMDb", width="stretch", key="plot_to_f4"):
        _switch_page("pages/04_omdb.py")

movie = api_get(f"/movies/{selected_id}")
review_stage = infer_review_stage(movie)

st.write("Estado traduccion:", movie.get("translation_status") or "")
if review_stage:
    st.caption(f"Origen de review detectado: `{review_stage}`")
if movie.get("translation_last_error"):
    st.warning(movie.get("translation_last_error"))
if movie.get("workflow_needs_review"):
    st.warning(movie.get("workflow_review_reason") or "Pendiente de revision")

st.markdown("### Plot original")
st.text_area("EN", value=movie.get("omdb_plot_en") or "", height=220, disabled=True)

plot_es = st.text_area("Plot en espanol", value=movie.get("omdb_plot_es") or "", height=220)

c1, c2 = st.columns(2)
with c1:
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
            st.success("Traduccion completada")
            st.json(result)
        except requests.exceptions.ReadTimeout:
            st.error("Timeout esperando al backend para este ID. Prueba Sidebar > HTTP timeout.")
        except Exception as exc:
            st.error(str(exc))

with c2:
    if st.button("Guardar traduccion manual"):
        try:
            api_put(f"/movies/{selected_id}/plot-es", json={"plot_es": plot_es})
            st.success("Traduccion guardada")
        except Exception as exc:
            st.error(str(exc))

st.divider()
st.subheader("Reejecucion acotada hasta review")
if not movie.get("workflow_needs_review"):
    st.info("Esta pelicula no esta en review.")
else:
    stage_target = review_stage or "translation"
    options = build_review_rerun_options(stage_target)
    option_labels = [label for label, _ in options]
    option_map = {label: start for label, start in options}
    selected_option = st.selectbox("Reejecucion disponible", option_labels, index=0, key="plot_rerun_option")
    selected_start_stage = option_map[selected_option]

    if st.button("Reejecutar workflow hasta review", key="plot_rerun_btn"):
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
