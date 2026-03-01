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
    )

configure_page()
st.title("Fase 4 - OMDb")
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
    omdb_review_only: bool,
) -> list[dict]:
    out = rows
    if review_only:
        out = [row for row in out if row.get("workflow_needs_review")]
    if omdb_review_only:
        out = [
            row for row in out if row.get("workflow_needs_review") and infer_review_stage(row) == "omdb"
        ]
    return out


movie_id = st.text_input("ID concreto (opcional)", value="")
limit = st.number_input("Limite batch", min_value=1, max_value=5000, value=20)
overwrite = st.checkbox("Refetch aunque ya exista", value=False)

if st.button("Descargar OMDb"):
    try:
        result = api_post(
            "/omdb/fetch",
            json={
                "movie_id": movie_id or None,
                "limit": int(limit),
                "overwrite": overwrite,
            },
            timeout=LONG_TIMEOUT_SECONDS,
        )
        st.success("Fetch OMDb completado")
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
rows_with_imdb = [row for row in rows if row.get("imdb_id")]
if not rows_with_imdb:
    st.info("No hay peliculas con IMDb ID")
    st.stop()

if "omdb_review_only_filter" not in st.session_state:
    st.session_state["omdb_review_only_filter"] = False
if "omdb_stage_review_only_filter" not in st.session_state:
    st.session_state["omdb_stage_review_only_filter"] = False

f1, f2, _ = st.columns([1, 1, 4])
with f1:
    review_label = "Mostrar todas" if st.session_state["omdb_review_only_filter"] else "Solo en review"
    if st.button(review_label, key="omdb_toggle_review"):
        st.session_state["omdb_review_only_filter"] = not st.session_state["omdb_review_only_filter"]
        st.rerun()
with f2:
    stage_label = (
        "Mostrar review global"
        if st.session_state["omdb_stage_review_only_filter"]
        else "Solo review de OMDb"
    )
    if st.button(stage_label, key="omdb_toggle_stage_review"):
        st.session_state["omdb_stage_review_only_filter"] = not st.session_state[
            "omdb_stage_review_only_filter"
        ]
        st.rerun()

filtered_rows = _filter_rows(
    rows_with_imdb,
    review_only=bool(st.session_state["omdb_review_only_filter"]),
    omdb_review_only=bool(st.session_state["omdb_stage_review_only_filter"]),
)
st.caption(f"Filtro actual: {len(filtered_rows)} de {len(rows_with_imdb)} peliculas")

if not filtered_rows:
    st.info("No hay peliculas con los filtros actuales.")
    st.stop()

selected_id = select_movie_id(filtered_rows, label="Pelicula", key="omdb_movie_selector")

nav_f2, nav_f3, nav_f5 = st.columns(3)
with nav_f2:
    if st.button("Ir a Fase 2 - Revision", width="stretch", key="omdb_to_f2"):
        _switch_page("pages/02_revision_titulo_equipo.py")
with nav_f3:
    if st.button("Ir a Fase 3 - IMDb", width="stretch", key="omdb_to_f3"):
        _switch_page("pages/03_imdb.py")
with nav_f5:
    if st.button("Ir a Fase 5 - Plot ES", width="stretch", key="omdb_to_f5"):
        _switch_page("pages/05_plot_es.py")

movie = api_get(f"/movies/{selected_id}")
review_stage = infer_review_stage(movie)

st.write("IMDb ID:", movie.get("imdb_id") or "")
st.write("Estado OMDb:", movie.get("omdb_status") or "")
if review_stage:
    st.caption(f"Origen de review detectado: `{review_stage}`")
if movie.get("omdb_last_error"):
    st.warning(movie.get("omdb_last_error"))
if movie.get("workflow_needs_review"):
    st.warning(movie.get("workflow_review_reason") or "Pendiente de revision")

with st.form("omdb_review"):
    omdb_title = st.text_input("Titulo", value=movie.get("omdb_title") or "")
    omdb_year = st.text_input("Year", value=movie.get("omdb_year") or "")
    omdb_runtime = st.text_input("Runtime", value=movie.get("omdb_runtime") or "")
    omdb_genre = st.text_input("Genre", value=movie.get("omdb_genre") or "")
    omdb_director = st.text_input("Director", value=movie.get("omdb_director") or "")
    omdb_actors = st.text_input("Actors", value=movie.get("omdb_actors") or "")
    omdb_language = st.text_input("Language", value=movie.get("omdb_language") or "")
    omdb_country = st.text_input("Country", value=movie.get("omdb_country") or "")
    omdb_plot_en = st.text_area("Plot (EN)", value=movie.get("omdb_plot_en") or "", height=180)

    save = st.form_submit_button("Guardar cambios")

if save:
    try:
        api_put(
            f"/movies/{selected_id}/omdb",
            json={
                "fields": {
                    "omdb_title": omdb_title,
                    "omdb_year": omdb_year,
                    "omdb_runtime": omdb_runtime,
                    "omdb_genre": omdb_genre,
                    "omdb_director": omdb_director,
                    "omdb_actors": omdb_actors,
                    "omdb_language": omdb_language,
                    "omdb_country": omdb_country,
                    "omdb_plot_en": omdb_plot_en,
                }
            },
        )
        st.success("OMDb actualizado")
    except Exception as exc:
        st.error(str(exc))

if movie.get("omdb_raw"):
    with st.expander("Ver OMDb raw"):
        st.json(movie["omdb_raw"])

st.divider()
st.subheader("Reejecucion acotada hasta review")
if not movie.get("workflow_needs_review"):
    st.info("Esta pelicula no esta en review.")
else:
    stage_target = review_stage or "omdb"
    options = build_review_rerun_options(stage_target)
    option_labels = [label for label, _ in options]
    option_map = {label: start for label, start in options}
    selected_option = st.selectbox("Reejecucion disponible", option_labels, index=0, key="omdb_rerun_option")
    selected_start_stage = option_map[selected_option]

    if st.button("Reejecutar workflow hasta review", key="omdb_rerun_btn"):
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
