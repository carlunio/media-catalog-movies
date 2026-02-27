import pandas as pd
import requests
import streamlit as st

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
st.title("Fase 3 - IMDb")
render_timeout_controls()

st.subheader("Busqueda automatica")
movie_id = st.text_input("ID concreto (opcional)", value="")
limit = st.number_input("Limite", min_value=1, max_value=5000, value=20)
overwrite = st.checkbox("Rebuscar aunque ya exista URL", value=False)
max_results = st.number_input("Resultados maximos por intento", min_value=1, max_value=20, value=10)

if st.button("Ejecutar busqueda IMDb"):
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
        st.success("Busqueda completada")
        st.json(result)
    except requests.exceptions.ReadTimeout:
        st.error(
            "Timeout esperando al backend. "
            "Reduce el limite batch o cambia el modo en Sidebar > HTTP timeout."
        )
    except Exception as exc:
        st.error(str(exc))

st.divider()

try:
    rows = api_get("/movies", params={"limit": 5000})
except Exception as exc:
    st.error(str(exc))
    st.stop()

if not rows:
    st.info("No hay peliculas")
    st.stop()

pending = [row for row in rows if not row.get("imdb_url")]
if pending:
    st.write(f"Pendientes IMDb: {len(pending)}")
    st.dataframe(pd.DataFrame(pending)[["id", "manual_title", "extraction_title", "imdb_status"]])

movie_ids = [row["id"] for row in rows]
selected_id = st.selectbox("Pelicula", movie_ids)
movie = api_get(f"/movies/{selected_id}")

st.markdown("### Datos base")
st.write("Titulo manual:", movie.get("manual_title") or "")
st.write("Titulo extraido:", movie.get("extraction_title") or "")
st.write("Equipo:", ", ".join(movie.get("manual_team") or movie.get("extraction_team") or []))
st.write("Query usada:", movie.get("imdb_query") or "")
st.write("Estado:", movie.get("imdb_status") or "")
if movie.get("imdb_last_error"):
    st.write("Ultimo error:", movie.get("imdb_last_error"))
st.write("Workflow:", movie.get("workflow_status") or "")
st.write("Etapa:", movie.get("pipeline_stage") or "")
if movie.get("workflow_needs_review"):
    st.warning(movie.get("workflow_review_reason") or "Pendiente de revision")

if movie.get("imdb_url"):
    st.markdown(f"IMDb actual: [{movie['imdb_url']}]({movie['imdb_url']})")

if st.button("Buscar IMDb solo para este ID"):
    try:
        result = api_post(
            "/imdb/search",
            json={"movie_id": selected_id, "limit": 1, "overwrite": True, "max_results": int(max_results)},
            timeout=LONG_TIMEOUT_SECONDS,
        )
        st.success("Busqueda completada")
        st.json(result)
    except requests.exceptions.ReadTimeout:
        st.error("Timeout esperando al backend para este ID. Prueba Sidebar > HTTP timeout.")
    except Exception as exc:
        st.error(str(exc))

manual_url = st.text_input("IMDb URL manual", value=movie.get("imdb_url") or "")
if st.button("Guardar URL manual IMDb"):
    try:
        api_put(f"/movies/{selected_id}/imdb", json={"imdb_url": manual_url})
        st.success("IMDb guardado")
    except Exception as exc:
        st.error(str(exc))
