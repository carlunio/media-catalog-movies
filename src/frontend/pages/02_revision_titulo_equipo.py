import requests
import streamlit as st
from PIL import Image, ImageOps

try:
    from src.frontend.utils import (
        LONG_TIMEOUT_SECONDS,
        api_get,
        api_post,
        api_put,
        configure_page,
        render_timeout_controls,
    )
except ModuleNotFoundError:  # pragma: no cover
    from frontend.utils import (
        LONG_TIMEOUT_SECONDS,
        api_get,
        api_post,
        api_put,
        configure_page,
        render_timeout_controls,
    )

configure_page()
st.title("Fase 2 - Revision titulo/equipo y acciones")
render_timeout_controls()


def _load_image_with_orientation(path: str):
    with Image.open(path) as image:
        # Respect EXIF orientation so portrait covers are not shown rotated.
        return ImageOps.exif_transpose(image).copy()


try:
    rows = api_get("/movies", params={"limit": 5000})
except Exception as exc:
    st.error(str(exc))
    st.stop()

if not rows:
    st.info("No hay peliculas cargadas.")
    st.stop()

movie_ids = [row["id"] for row in rows]

if "movie_idx" not in st.session_state:
    st.session_state["movie_idx"] = 0

selected_id = st.selectbox("Selecciona pelicula", movie_ids, index=st.session_state["movie_idx"])
st.session_state["movie_idx"] = movie_ids.index(selected_id)

col_l, col_r = st.columns(2)
with col_l:
    if st.button("Anterior", disabled=st.session_state["movie_idx"] == 0):
        st.session_state["movie_idx"] -= 1
        st.rerun()
with col_r:
    if st.button("Siguiente", disabled=st.session_state["movie_idx"] == len(movie_ids) - 1):
        st.session_state["movie_idx"] += 1
        st.rerun()

st.caption(f"Registro {st.session_state['movie_idx'] + 1} de {len(movie_ids)}")

movie = api_get(f"/movies/{selected_id}")

left, right = st.columns([1, 2])

with left:
    st.write(f"ID: {selected_id}")
    if movie.get("image_path"):
        try:
            st.image(_load_image_with_orientation(movie["image_path"]), use_container_width=True)
        except (FileNotFoundError, OSError) as exc:
            st.warning(f"No se pudo cargar la imagen: {exc}")

with right:
    st.markdown("### Extraccion")
    st.write("Titulo extraido:", movie.get("extraction_title") or "")
    st.write("Equipo extraido:", ", ".join(movie.get("extraction_team", [])))

    default_title = movie.get("manual_title") or movie.get("extraction_title") or ""
    default_team = ", ".join(movie.get("manual_team") or movie.get("extraction_team") or [])

    with st.form("review_form"):
        title = st.text_input("Titulo revisado", value=default_title)
        team_text = st.text_area("Equipo revisado (coma o salto de linea)", value=default_team, height=120)
        save = st.form_submit_button("Guardar cambios")

    if save:
        team = [part.strip() for part in team_text.replace("\n", ",").split(",") if part.strip()]
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

st.subheader("Acciones LangGraph")
a1, a2, a3, a4 = st.columns(4)

with a1:
    if st.button("Aprobar ficha"):
        try:
            result = api_post(
                f"/workflow/review/{selected_id}",
                json={"action": "approve"},
                timeout=LONG_TIMEOUT_SECONDS,
            )
            st.success("Ficha aprobada")
            st.json(result)
        except requests.exceptions.ReadTimeout:
            st.error("Timeout al aprobar ficha")
        except Exception as exc:
            st.error(str(exc))

with a2:
    if st.button("Reintentar desde extraccion"):
        try:
            result = api_post(
                f"/workflow/review/{selected_id}",
                json={"action": "retry_from_extraction"},
                timeout=LONG_TIMEOUT_SECONDS,
            )
            st.success("Workflow relanzado desde extraccion")
            st.json(result)
        except requests.exceptions.ReadTimeout:
            st.error("Timeout relanzando workflow")
        except Exception as exc:
            st.error(str(exc))

with a3:
    if st.button("Reintentar desde IMDb"):
        try:
            result = api_post(
                f"/workflow/review/{selected_id}",
                json={"action": "retry_from_imdb"},
                timeout=LONG_TIMEOUT_SECONDS,
            )
            st.success("Workflow relanzado desde IMDb")
            st.json(result)
        except requests.exceptions.ReadTimeout:
            st.error("Timeout relanzando workflow")
        except Exception as exc:
            st.error(str(exc))

with a4:
    if st.button("Reintentar desde OMDb"):
        try:
            result = api_post(
                f"/workflow/review/{selected_id}",
                json={"action": "retry_from_omdb"},
                timeout=LONG_TIMEOUT_SECONDS,
            )
            st.success("Workflow relanzado desde OMDb")
            st.json(result)
        except requests.exceptions.ReadTimeout:
            st.error("Timeout relanzando workflow")
        except Exception as exc:
            st.error(str(exc))
