import streamlit as st

try:
    from src.frontend.utils import render_timeout_controls, show_backend_status
except ModuleNotFoundError:  # pragma: no cover
    from frontend.utils import render_timeout_controls, show_backend_status

st.set_page_config(page_title="Media Catalog Movies", layout="wide")

st.title("Media Catalog Movies")
render_timeout_controls()
st.markdown(
    """
Pipeline de catalogacion de peliculas:

0. Orquestacion LangGraph (grafo, estados, cola de review).
1. Ingesta de caratulas.
2. Revision manual con portada visible.
3. Enlace IMDb.
4. Ficha OMDb.
5. Traduccion del plot.
6. Exportacion.
"""
)

show_backend_status()
st.info("Usa el menu lateral para recorrer cada fase.")
