import requests
import streamlit as st
from PIL import Image, ImageOps

try:
    from src.frontend.utils import (
        LONG_TIMEOUT_SECONDS,
        WORKFLOW_STAGES,
        api_get,
        api_post,
        api_put,
        build_review_rerun_options,
        configure_page,
        render_icon_heading,
        infer_review_stage,
        node_ui_label,
        render_timeout_controls,
        select_movie_id,
        stage_ui_label,
        set_selected_movie_id,
    )
except ModuleNotFoundError:  # pragma: no cover
    from frontend.utils import (
        LONG_TIMEOUT_SECONDS,
        WORKFLOW_STAGES,
        api_get,
        api_post,
        api_put,
        build_review_rerun_options,
        configure_page,
        render_icon_heading,
        infer_review_stage,
        node_ui_label,
        render_timeout_controls,
        select_movie_id,
        stage_ui_label,
        set_selected_movie_id,
    )

configure_page()
render_icon_heading("Fase 2 - Título", icon="pen-to-square", level=1)
render_timeout_controls()


def _load_image_with_orientation(path: str):
    with Image.open(path) as image:
        # Respect EXIF orientation so portrait covers are not shown rotated.
        return ImageOps.exif_transpose(image).copy()


def _switch_page(target: str) -> None:
    switch_fn = getattr(st, "switch_page", None)
    if callable(switch_fn):
        switch_fn(target)
    else:
        st.info("Tu versión de Streamlit no soporta `switch_page`.")


def _parse_team_input(team_text: str) -> list[str]:
    raw = str(team_text or "").replace("\r", "").strip()
    if not raw:
        return []

    # Multi-movie mode: each movie team block separated with ';'
    if ";" in raw:
        return [part.strip() for part in raw.split(";") if part.strip()]

    return [part.strip() for part in raw.replace("\n", ",").split(",") if part.strip()]


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
            if row.get("workflow_needs_review") and infer_review_stage(row) == "extraction"
        ]
    return rows


try:
    rows = api_get("/movies", params={"limit": 5000})
except Exception as exc:
    st.error(str(exc))
    st.stop()

if not rows:
    st.info("No hay películas cargadas.")
    st.stop()

mode_labels = {
    "all": "Mostrar todas",
    "review": "Solo en revisión",
    "review_stage": "Solo revisión de extracción",
}
if "review_title_filter_mode" not in st.session_state:
    st.session_state["review_title_filter_mode"] = "all"

filter_mode = st.segmented_control(
    "Filtro",
    options=list(mode_labels.keys()),
    default=st.session_state["review_title_filter_mode"],
    format_func=lambda value: mode_labels.get(str(value), str(value)),
    key="review_title_filter_mode",
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

selected_id = select_movie_id(
    filtered_rows,
    label="Selecciona película",
    key="review_title_movie_selector",
)
movie_ids = [row["id"] for row in filtered_rows]
current_index = movie_ids.index(selected_id)

col_prev, col_next = st.columns(2)
with col_prev:
    if st.button("Anterior", disabled=current_index == 0, key="review_title_prev"):
        set_selected_movie_id(movie_ids[current_index - 1])
        st.rerun()
with col_next:
    if st.button("Siguiente", disabled=current_index == len(movie_ids) - 1, key="review_title_next"):
        set_selected_movie_id(movie_ids[current_index + 1])
        st.rerun()

st.caption(f"Registro {current_index + 1} de {len(movie_ids)}")

nav_imdb, nav_omdb, nav_plot = st.columns(3)
with nav_imdb:
    if st.button("Ir a Fase 3 - IMDb", width="stretch", key="review_title_to_imdb"):
        _switch_page("pages/03_IMDb.py")
with nav_omdb:
    if st.button("Ir a Fase 4 - OMDb", width="stretch", key="review_title_to_omdb"):
        _switch_page("pages/04_OMDb.py")
with nav_plot:
    if st.button("Ir a Fase 5 - Sinopsis ES", width="stretch", key="review_title_to_plot"):
        _switch_page("pages/05_Sinopsis_ES.py")

movie = api_get(f"/movies/{selected_id}")
review_stage = infer_review_stage(movie)

left, right = st.columns([1, 2])

with left:
    st.write(f"ID: {selected_id}")
    if movie.get("image_path"):
        try:
            st.image(_load_image_with_orientation(movie["image_path"]), width="stretch")
        except (FileNotFoundError, OSError) as exc:
            st.warning(f"No se pudo cargar la imagen: {exc}")

with right:
    render_icon_heading("Extracción", icon="tag", level=3)
    st.write("Título extraído:", movie.get("extraction_title") or "")
    st.write("Equipo extraido:", ", ".join(movie.get("extraction_team", [])))
    if review_stage:
        st.caption(f"Origen de revisión detectado: `{stage_ui_label(review_stage)}`")

    default_title = movie.get("manual_title") or movie.get("extraction_title") or ""
    default_team = ", ".join(movie.get("manual_team") or movie.get("extraction_team") or [])

    with st.form("review_form"):
        title = st.text_input("Título revisado", value=default_title)
        team_text = st.text_area(
            "Equipo revisado (coma/salto de línea; usa ';' para separar por película)",
            value=default_team,
            height=120,
        )
        save = st.form_submit_button("Guardar cambios")

    if save:
        team = _parse_team_input(team_text)
        try:
            api_put(
                f"/movies/{selected_id}/title-team",
                json={"title": title, "team": team},
            )
            st.success("Guardado")
        except Exception as exc:
            st.error(str(exc))

st.divider()

m1, m2, m3, m4 = st.columns(4)
m1.metric("IMDb", movie.get("imdb_status") or "")
m2.metric("OMDb", movie.get("omdb_status") or "")
m3.metric("Sinopsis ES", movie.get("translation_status") or "")
m4.metric("Workflow", movie.get("workflow_status") or "")
st.caption(
    f"Etapa actual: `{stage_ui_label(movie.get('pipeline_stage') or 'unknown')}` | "
    f"Nodo actual: `{node_ui_label(movie.get('workflow_current_node') or '-')}` | "
    f"Intento: `{movie.get('workflow_attempt') or 0}`"
)

if movie.get("workflow_needs_review"):
    st.warning(movie.get("workflow_review_reason") or "Requiere revisión")

render_icon_heading("Acciones LangGraph para revisión", icon="gears", level=2)

if not movie.get("workflow_needs_review"):
    st.info("Esta película no está en estado de revisión. No hay reejecución acotada disponible.")
else:
    if review_stage is None:
        st.warning("No se pudo inferir la fase de revisión automáticamente.")
        review_stage = st.selectbox(
            "Fase objetivo de revisión",
            list(WORKFLOW_STAGES),
            index=0,
            format_func=stage_ui_label,
        )
    else:
        st.caption(f"Fase de revisión detectada: `{stage_ui_label(review_stage)}`")

    options = build_review_rerun_options(review_stage)
    option_labels = [label for label, _ in options]
    option_map = {label: start for label, start in options}
    selected_option = st.selectbox("Reejecución disponible", option_labels, index=0)
    selected_start_stage = option_map[selected_option]

    if st.button("Reejecutar workflow hasta la fase de revisión"):
        payload = {
            "movie_id": selected_id,
            "limit": 1,
            "start_stage": selected_start_stage,
            "stop_after": review_stage,
            "overwrite": True,
        }
        try:
            result = api_post(
                "/workflow/run",
                json=payload,
                timeout=LONG_TIMEOUT_SECONDS,
            )
            st.success(
                f"Workflow relanzado desde {stage_ui_label(selected_start_stage)} "
                f"hasta {stage_ui_label(review_stage)} para {selected_id}."
            )
            st.json(result)
        except requests.exceptions.ReadTimeout:
            st.error("Timeout relanzando workflow")
        except Exception as exc:
            st.error(str(exc))
