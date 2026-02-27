import os

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
        select_ollama_model,
    )
except ModuleNotFoundError:  # pragma: no cover
    from frontend.utils import (
        LONG_TIMEOUT_SECONDS,
        api_get,
        api_post,
        api_put,
        configure_page,
        render_timeout_controls,
        select_ollama_model,
    )

configure_page()
st.title("Fase 5 - Traduccion plot")
render_timeout_controls()

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

selected_id = st.selectbox("Pelicula", [r["id"] for r in rows_with_plot])
movie = api_get(f"/movies/{selected_id}")

st.write("Estado traduccion:", movie.get("translation_status") or "")
if movie.get("translation_last_error"):
    st.warning(movie.get("translation_last_error"))

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
