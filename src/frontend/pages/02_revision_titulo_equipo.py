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
        infer_review_stage,
        render_timeout_controls,
        select_movie_id,
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
        infer_review_stage,
        render_timeout_controls,
        select_movie_id,
        set_selected_movie_id,
    )

configure_page()
st.title("Fase 2 - Revision titulo/equipo y acciones")
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
        st.info("Tu version de Streamlit no soporta switch_page.")


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
    review_only: bool,
    extraction_review_only: bool,
) -> list[dict]:
    out = rows
    if review_only:
        out = [row for row in out if row.get("workflow_needs_review")]
    if extraction_review_only:
        out = [
            row
            for row in out
            if row.get("workflow_needs_review") and infer_review_stage(row) == "extraction"
        ]
    return out


try:
    rows = api_get("/movies", params={"limit": 5000})
except Exception as exc:
    st.error(str(exc))
    st.stop()

if not rows:
    st.info("No hay peliculas cargadas.")
    st.stop()

if "review_only_filter" not in st.session_state:
    st.session_state["review_only_filter"] = False
if "review_extraction_only_filter" not in st.session_state:
    st.session_state["review_extraction_only_filter"] = False

filter_col1, filter_col2, _ = st.columns([1, 1, 4])
with filter_col1:
    review_label = "Mostrar todas" if st.session_state["review_only_filter"] else "Solo en review"
    if st.button(review_label, key="review_title_toggle_review"):
        st.session_state["review_only_filter"] = not st.session_state["review_only_filter"]
        st.rerun()
with filter_col2:
    extraction_label = (
        "Mostrar review global"
        if st.session_state["review_extraction_only_filter"]
        else "Solo review de extraccion"
    )
    if st.button(extraction_label, key="review_title_toggle_extraction"):
        st.session_state["review_extraction_only_filter"] = not st.session_state[
            "review_extraction_only_filter"
        ]
        st.rerun()

filtered_rows = _filter_rows(
    rows,
    review_only=bool(st.session_state["review_only_filter"]),
    extraction_review_only=bool(st.session_state["review_extraction_only_filter"]),
)
st.caption(f"Filtro actual: {len(filtered_rows)} de {len(rows)} peliculas")

if not filtered_rows:
    st.info("No hay peliculas con los filtros actuales.")
    st.stop()

selected_id = select_movie_id(
    filtered_rows,
    label="Selecciona pelicula",
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
        _switch_page("pages/03_imdb.py")
with nav_omdb:
    if st.button("Ir a Fase 4 - OMDb", width="stretch", key="review_title_to_omdb"):
        _switch_page("pages/04_omdb.py")
with nav_plot:
    if st.button("Ir a Fase 5 - Plot ES", width="stretch", key="review_title_to_plot"):
        _switch_page("pages/05_plot_es.py")

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
    st.markdown("### Extraccion")
    st.write("Titulo extraido:", movie.get("extraction_title") or "")
    st.write("Equipo extraido:", ", ".join(movie.get("extraction_team", [])))
    if review_stage:
        st.caption(f"Origen de review detectado: `{review_stage}`")

    default_title = movie.get("manual_title") or movie.get("extraction_title") or ""
    default_team = ", ".join(movie.get("manual_team") or movie.get("extraction_team") or [])

    with st.form("review_form"):
        title = st.text_input("Titulo revisado", value=default_title)
        team_text = st.text_area(
            "Equipo revisado (coma/salto de linea; usa ';' para separar por pelicula)",
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
m3.metric("Plot ES", movie.get("translation_status") or "")
m4.metric("Workflow", movie.get("workflow_status") or "")
st.caption(
    f"Etapa actual: `{movie.get('pipeline_stage') or 'unknown'}` | "
    f"Nodo actual: `{movie.get('workflow_current_node') or '-'}` | "
    f"Intento: `{movie.get('workflow_attempt') or 0}`"
)

if movie.get("workflow_needs_review"):
    st.warning(movie.get("workflow_review_reason") or "Requiere revision")

st.subheader("Acciones LangGraph para review")

if not movie.get("workflow_needs_review"):
    st.info("Esta pelicula no esta en estado review. No hay reejecucion acotada disponible.")
else:
    if review_stage is None:
        st.warning("No se pudo inferir la fase de review automaticamente.")
        review_stage = st.selectbox("Fase objetivo de review", list(WORKFLOW_STAGES), index=0)
    else:
        st.caption(f"Fase de review detectada: `{review_stage}`")

    options = build_review_rerun_options(review_stage)
    option_labels = [label for label, _ in options]
    option_map = {label: start for label, start in options}
    selected_option = st.selectbox("Reejecucion disponible", option_labels, index=0)
    selected_start_stage = option_map[selected_option]

    if st.button("Reejecutar workflow hasta la fase de review"):
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
                f"Workflow relanzado desde {selected_start_stage} "
                f"hasta {review_stage} para {selected_id}."
            )
            st.json(result)
        except requests.exceptions.ReadTimeout:
            st.error("Timeout relanzando workflow")
        except Exception as exc:
            st.error(str(exc))
