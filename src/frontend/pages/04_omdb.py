import requests
import streamlit as st

try:
    from src.frontend.utils import LONG_TIMEOUT_SECONDS, api_get, api_post, api_put
except ModuleNotFoundError:  # pragma: no cover
    from frontend.utils import LONG_TIMEOUT_SECONDS, api_get, api_post, api_put

st.title("Fase 4 - OMDb")

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
            "Reduce el limite batch o aumenta API_LONG_TIMEOUT_SECONDS en tu .env y reinicia Streamlit."
        )
    except Exception as exc:
        st.error(str(exc))

st.divider()

rows = api_get("/movies", params={"limit": 5000})
rows_with_imdb = [row for row in rows if row.get("imdb_id")]
if not rows_with_imdb:
    st.info("No hay peliculas con IMDb ID")
    st.stop()

selected_id = st.selectbox("Pelicula", [r["id"] for r in rows_with_imdb])
movie = api_get(f"/movies/{selected_id}")

st.write("IMDb ID:", movie.get("imdb_id") or "")
st.write("Estado OMDb:", movie.get("omdb_status") or "")
if movie.get("omdb_last_error"):
    st.warning(movie.get("omdb_last_error"))

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
