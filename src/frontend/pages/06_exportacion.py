from pathlib import Path

import streamlit as st

try:
    from src.frontend.utils import api_get, configure_page, render_timeout_controls
except ModuleNotFoundError:  # pragma: no cover
    from frontend.utils import api_get, configure_page, render_timeout_controls

configure_page()
st.title("Fase 6 - Exportacion")
render_timeout_controls()

if st.button("Exportar a TSV"):
    try:
        result = api_get("/export/movies/tsv")
        path = Path(result["path"])
        if not path.exists():
            st.error(f"No se encontro archivo exportado: {path}")
        else:
            st.success(f"Archivo generado: {path}")
            content = path.read_text(encoding="utf-8")
            st.download_button(
                label="Descargar movies.tsv",
                data=content,
                file_name="movies.tsv",
                mime="text/tab-separated-values",
            )
    except Exception as exc:
        st.error(str(exc))
