import streamlit as st

try:
    from src.frontend.utils import show_backend_status
except ModuleNotFoundError:  # pragma: no cover
    from frontend.utils import show_backend_status

st.set_page_config(page_title="Media Catalog Movies", layout="wide")

st.title("Media Catalog Movies")
st.markdown(
    """
Pipeline de catalogacion de peliculas:

1. Ingesta de caratulas.
2. Extraccion automatica (titulo + equipo).
3. Revision manual con portada visible.
4. Enlace IMDb.
5. Ficha OMDb.
6. Traduccion del plot.
7. Exportacion.
"""
)

show_backend_status()
st.info("Usa el menu lateral para recorrer cada fase.")
