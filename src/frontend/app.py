from pathlib import Path

import streamlit as st

try:
    from src.frontend.utils import (
        configure_page,
        get_selected_movie_id,
        render_timeout_controls,
        show_backend_status,
    )
except ModuleNotFoundError:  # pragma: no cover
    from frontend.utils import configure_page, get_selected_movie_id, render_timeout_controls, show_backend_status

configure_page()

st.title("Media Catalog Movies")
render_timeout_controls()
st.markdown(
    """
Pipeline de catalogacion de peliculas:

0. Orquestacion LangGraph (grafo, estados, cola de review).
1. Ingesta de caratulas.
2. Revision manual con portada visible.
3. Enlace IMDb.
3b. Titulo en espanol desde IMDb.
4. Ficha OMDb.
5. Traduccion del plot.
6. Exportacion.
"""
)

show_backend_status()
st.info("Usa el menu lateral para recorrer cada fase.")

selected_movie = get_selected_movie_id()
if selected_movie:
    st.sidebar.caption(f"Pelicula seleccionada: {selected_movie}")

with st.sidebar.expander("Debug runtime", expanded=False):
    st.code(
        "\n".join(
            [
                f"script={Path(__file__).resolve()}",
                f"cwd={Path.cwd().resolve()}",
            ]
        ),
        language="text",
    )
