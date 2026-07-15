from pathlib import Path

import streamlit as st

try:
    from src.frontend.utils import (
        configure_page,
        render_icon_heading,
        get_selected_movie_id,
        load_cover_name_audit,
        render_timeout_controls,
        show_backend_status,
    )
except ModuleNotFoundError:  # pragma: no cover
    from frontend.utils import (
        configure_page,
        render_icon_heading,
        get_selected_movie_id,
        load_cover_name_audit,
        render_timeout_controls,
        show_backend_status,
    )

configure_page()

render_icon_heading("Media Catalog Movies", icon="film", level=1)
render_timeout_controls()
st.markdown(
    """
Pipeline de catalogación de películas:

0. Orquestación LangGraph (grafo, estados, cola de revisión).
1. Lectura de carátulas.
2. Título.
3. Enlace IMDb.
3b. Título en español desde IMDb.
4. Ficha OMDb.
5. Traducción de la sinopsis.
6. Formulario.
7. Exportación.
8. Datos y snapshots.
"""
)

show_backend_status()

try:
    audit = load_cover_name_audit()
    invalid_files = int(audit.get("invalid_cover_files_count", 0))
    invalid_db_ids = int(audit.get("invalid_db_ids_count", 0))
    if invalid_files or invalid_db_ids:
        st.warning(
            "Detección automática de nombres: "
            f"{invalid_files} carátulas y {invalid_db_ids} IDs en BBDD no cumplen `PNNNN`."
        )
        with st.expander("Ver detalle de nombres inválidos", expanded=False):
            st.write("Patron esperado:", audit.get("expected_pattern", "PNNNN"))
            bad_file_ids = audit.get("invalid_cover_ids_unique", [])
            bad_db_ids = audit.get("invalid_db_ids_preview", [])
            if bad_file_ids:
                st.write("IDs inválidos detectados en nombres de imagen:", ", ".join(bad_file_ids[:100]))
            if bad_db_ids:
                st.write("IDs inválidos detectados en BBDD:", ", ".join(bad_db_ids[:100]))
except Exception:
    # Do not block app startup if audit endpoint is not available.
    pass

st.info("Usa el menu lateral para recorrer cada fase.")

selected_movie = get_selected_movie_id()
if selected_movie:
    st.sidebar.caption(f"Película seleccionada: {selected_movie}")

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
