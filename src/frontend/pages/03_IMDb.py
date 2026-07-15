import pandas as pd
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
        node_ui_label,
        render_timeout_controls,
        select_movie_id,
        stage_ui_label,
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
        node_ui_label,
        render_timeout_controls,
        select_movie_id,
        stage_ui_label,
    )

configure_page()
render_icon_heading("Fase 3 - IMDb", icon="magnifying-glass", level=1)
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
            if row.get("workflow_needs_review")
            and infer_review_stage(row) in {"imdb", "title_es"}
        ]
    return rows


def _value_parts(text: str | None) -> int:
    parts = [part.strip() for part in str(text or "").split(";") if part.strip()]
    return len(parts)


def _needs_imdb_title_fields(row: dict) -> bool:
    manual_title_es = str(row.get("imdb_title_es") or "").strip()
    if (
        str(row.get("imdb_title_es_status") or "").strip().lower() == "manual"
        and manual_title_es
    ):
        return False

    imdb_url = str(row.get("imdb_url") or "").strip()
    if not imdb_url:
        return False

    urls_count = _value_parts(imdb_url)
    title_es_count = _value_parts(row.get("imdb_title_es"))

    if title_es_count == 0:
        return True
    if urls_count > 1 and title_es_count != urls_count:
        return True
    return False


def _with_ui_stage(rows: list[dict]) -> list[dict]:
    out: list[dict] = []
    for row in rows:
        clone = dict(row)
        clone["pipeline_stage_ui"] = stage_ui_label(row.get("pipeline_stage"))
        out.append(clone)
    return out

render_icon_heading("Acciones por lote (opcional)", icon="list", level=2)
with st.expander("Búsqueda y extracción por lote", expanded=False):
    batch_c1, batch_c2, batch_c3, batch_c4 = st.columns([2, 1, 1, 1])
    with batch_c1:
        movie_id = st.text_input("ID concreto (opcional)", value="")
    with batch_c2:
        limit = st.number_input("Límite", min_value=1, max_value=5000, value=20)
    with batch_c3:
        max_results = st.number_input("Max resultados IMDb", min_value=1, max_value=20, value=10)
    with batch_c4:
        overwrite = st.checkbox("Rebuscar URL", value=False)

    run_col1, run_col2 = st.columns(2)
    with run_col1:
        if st.button("Ejecutar búsqueda IMDb"):
            try:
                result = api_post(
                    "/imdb/search",
                    json={
                        "movie_id": movie_id or None,
                        "limit": int(limit),
                        "overwrite": overwrite,
                        "max_results": int(max_results),
                    },
                    timeout=LONG_TIMEOUT_SECONDS,
                )
                st.success("Búsqueda completada")
                st.json(result)
            except requests.exceptions.ReadTimeout:
                st.error(
                    "Timeout esperando al backend. "
                    "Reduce el límite del lote o cambia el modo en Sidebar > HTTP timeout."
                )
            except Exception as exc:
                st.error(str(exc))

    with run_col2:
        overwrite_title_es = st.checkbox("Reextraer título ES", value=False)
        if st.button("Extraer título ES IMDb (batch)"):
            payload = {
                "movie_id": movie_id or None,
                "limit": int(limit),
                "start_stage": "title_es",
                "stop_after": "title_es",
                "overwrite": bool(overwrite_title_es),
                "max_results": int(max_results),
            }
            try:
                result = api_post("/workflow/run", json=payload, timeout=LONG_TIMEOUT_SECONDS)
                st.success("Extracción de título ES completada")
                st.json(result)
            except requests.exceptions.ReadTimeout:
                st.error("Timeout extrayendo título ES IMDb.")
            except Exception as exc:
                st.error(str(exc))

st.divider()

try:
    rows = api_get("/movies", params={"limit": 5000})
except Exception as exc:
    st.error(str(exc))
    st.stop()

if not rows:
    st.info("No hay películas")
    st.stop()

with st.expander("Pendientes de fase IMDb", expanded=False):
    pending = [row for row in rows if not row.get("imdb_url")]
    if pending:
        st.write(f"Pendientes IMDb: {len(pending)}")
        pending_df_rows = _with_ui_stage(pending)
        st.dataframe(
            pd.DataFrame(pending_df_rows)[
                ["id", "pipeline_stage_ui", "manual_title", "extraction_title", "imdb_status"]
            ].rename(columns={"pipeline_stage_ui": "pipeline_stage"}),
            width="stretch",
        )
    else:
        st.caption("Sin pendientes de búsqueda IMDb.")

    pending_title_es = [row for row in rows if _needs_imdb_title_fields(row)]
    if pending_title_es:
        st.write(f"Pendientes título ES IMDb: {len(pending_title_es)}")
        pending_titles_df_rows = _with_ui_stage(pending_title_es)
        st.dataframe(
            pd.DataFrame(pending_titles_df_rows)[
                [
                    "id",
                    "pipeline_stage_ui",
                    "imdb_status",
                    "imdb_title_es_status",
                    "imdb_title_es_last_error",
                ]
            ].rename(columns={"pipeline_stage_ui": "pipeline_stage"}),
            width="stretch",
        )
    else:
        st.caption("Sin pendientes de título ES.")

mode_labels = {
    "all": "Mostrar todas",
    "review": "Solo en revisión",
    "review_stage": "Solo revisión IMDb/Título ES",
}
if "imdb_filter_mode" not in st.session_state:
    st.session_state["imdb_filter_mode"] = "all"

filter_mode = st.segmented_control(
    "Filtro",
    options=list(mode_labels.keys()),
    default=st.session_state["imdb_filter_mode"],
    format_func=lambda value: mode_labels.get(str(value), str(value)),
    key="imdb_filter_mode",
    width="stretch",
)
if filter_mode is None:
    filter_mode = "all"

filtered_rows = _filter_rows(
    rows,
    mode=str(filter_mode),
)
st.caption(
    f"Filtro actual: {mode_labels.get(str(filter_mode), 'Mostrar todas')} | "
    f"{len(filtered_rows)} de {len(rows)} películas"
)

if not filtered_rows:
    st.info("No hay películas con los filtros actuales.")
    st.stop()

selected_id = select_movie_id(filtered_rows, label="Película", key="imdb_movie_selector")
render_movie_prev_next(filtered_rows, selected_id, key_prefix="imdb_movie")


nav_f2, nav_f4, nav_f5 = st.columns(3)
with nav_f2:
    if st.button("Ir a Fase 2 - Título", width="stretch", key="imdb_to_f2"):
        _switch_page("pages/02_Título.py")
with nav_f4:
    if st.button("Ir a Fase 4 - OMDb", width="stretch", key="imdb_to_f4"):
        _switch_page("pages/04_OMDb.py")
with nav_f5:
    if st.button("Ir a Fase 5 - Sinopsis ES", width="stretch", key="imdb_to_f5"):
        _switch_page("pages/05_Sinopsis_ES.py")

movie = api_get(f"/movies/{selected_id}")
review_stage = infer_review_stage(movie)

render_icon_heading("Acciones sobre película seleccionada", icon="film", level=2)
left, right = st.columns([1.1, 1])
with left:
    render_icon_heading("Datos base", icon="table", level=3)
    st.write("Título manual:", movie.get("manual_title") or "")
    st.write("Título extraído:", movie.get("extraction_title") or "")
    st.write("Equipo:", ", ".join(movie.get("manual_team") or movie.get("extraction_team") or []))
    st.write("Query usada:", movie.get("imdb_query") or "")
    st.write("Estado IMDb:", movie.get("imdb_status") or "")
    if movie.get("imdb_last_error"):
        st.write("Último error IMDb:", movie.get("imdb_last_error"))

    st.write("Título ES IMDb:", movie.get("imdb_title_es") or "")
    st.write("Estado del título ES:", movie.get("imdb_title_es_status") or "")
    if movie.get("imdb_title_es_last_error"):
        st.write("Último error título ES:", movie.get("imdb_title_es_last_error"))

    st.write("Workflow:", movie.get("workflow_status") or "")
    st.write("Etapa:", stage_ui_label(movie.get("pipeline_stage") or ""))
    st.write("Nodo:", node_ui_label(movie.get("workflow_current_node") or ""))
    if review_stage:
        st.caption(f"Origen de revisión detectado: `{stage_ui_label(review_stage)}`")
    if movie.get("workflow_needs_review"):
        st.warning(movie.get("workflow_review_reason") or "Pendiente de revisión")

with right:
    render_icon_heading("Acciones", icon="sliders", level=3)
    imdb_url = str(movie.get("imdb_url") or "").strip()
    if imdb_url:
        st.markdown(f"[Abrir IMDb en pestaña]({imdb_url})")

    action_c1, action_c2 = st.columns(2)
    with action_c1:
        if st.button("Buscar IMDb solo este ID", width="stretch"):
            try:
                result = api_post(
                    "/imdb/search",
                    json={"movie_id": selected_id, "limit": 1, "overwrite": True, "max_results": int(max_results)},
                    timeout=LONG_TIMEOUT_SECONDS,
                )
                st.success("Búsqueda completada")
                st.json(result)
            except requests.exceptions.ReadTimeout:
                st.error("Timeout esperando al backend para este ID. Prueba Sidebar > HTTP timeout.")
            except Exception as exc:
                st.error(str(exc))
    with action_c2:
        if st.button("Extraer título ES solo este ID", width="stretch"):
            try:
                result = api_post(
                    "/workflow/run",
                    json={
                        "movie_id": selected_id,
                        "limit": 1,
                        "start_stage": "title_es",
                        "stop_after": "title_es",
                        "overwrite": True,
                    },
                    timeout=LONG_TIMEOUT_SECONDS,
                )
                st.success("Extracción de título ES completada")
                st.json(result)
            except requests.exceptions.ReadTimeout:
                st.error("Timeout extrayendo título ES para este ID.")
            except Exception as exc:
                st.error(str(exc))

    url_c1, url_c2 = st.columns([3, 1])
    with url_c1:
        manual_url = st.text_input(
            "IMDb URL manual (usa ';' para varias películas)",
            value=movie.get("imdb_url") or "",
            key=f"imdb_{selected_id}_manual_url",
        )
    with url_c2:
        if st.button("Guardar URL", width="stretch"):
            try:
                api_put(f"/movies/{selected_id}/imdb", json={"imdb_url": manual_url})
                st.success("IMDb guardado")
            except Exception as exc:
                st.error(str(exc))

    es_c1, _ = st.columns([2, 3])
    with es_c1:
        manual_title_es = st.text_input(
            "Título ES manual",
            value=movie.get("imdb_title_es") or "",
            key=f"imdb_{selected_id}_manual_title_es",
        )

    save_c1, _ = st.columns([1.8, 3.2])
    with save_c1:
        if st.button("Guardar título ES", width="stretch"):
            try:
                api_put(
                    f"/movies/{selected_id}/imdb-title-es",
                    json={"title_es": manual_title_es},
                )
                st.success("Título ES guardado")
            except Exception as exc:
                st.error(str(exc))

st.divider()
render_icon_heading("Reejecución acotada hasta revisión", icon="rotate-right", level=2)
if not movie.get("workflow_needs_review"):
    st.info("Esta película no está en revisión.")
else:
    stage_target = review_stage or "imdb"
    options = build_review_rerun_options(stage_target)
    option_labels = [label for label, _ in options]
    option_map = {label: start for label, start in options}
    selected_option = st.selectbox("Reejecución disponible", option_labels, index=0, key="imdb_rerun_option")
    selected_start_stage = option_map[selected_option]

    if st.button("Reejecutar workflow hasta revisión", key="imdb_rerun_btn"):
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
