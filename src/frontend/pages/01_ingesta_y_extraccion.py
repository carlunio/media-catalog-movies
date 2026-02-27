import os

import pandas as pd
import streamlit as st

try:
    from src.frontend.utils import api_get, api_post, configure_page, load_stats, render_timeout_controls
except ModuleNotFoundError:  # pragma: no cover
    from frontend.utils import api_get, api_post, configure_page, load_stats, render_timeout_controls

configure_page()
st.title("Fase 1 - Ingesta")
render_timeout_controls()

stats = load_stats()
c1, c2, c3, c4, c5, c6, c7 = st.columns(7)
c1.metric("Total", stats.get("total", 0))
c2.metric("Sin extraccion", stats.get("needs_extraction", 0))
c3.metric("Sin revision", stats.get("needs_manual_review", 0))
c4.metric("Sin IMDb", stats.get("needs_imdb", 0))
c5.metric("Sin OMDb", stats.get("needs_omdb", 0))
c6.metric("Sin traduccion", stats.get("needs_translation", 0))
c7.metric("Pendiente revision", stats.get("needs_workflow_review", 0))

st.divider()

st.subheader("Ingestar caratulas")
default_folder = os.getenv("COVERS_DIR", "data/input")
folder = st.text_input("Carpeta origen", value=default_folder)
recursive = st.checkbox("Buscar recursivamente", value=True)
overwrite_paths = st.checkbox("Sobrescribir ruta si el ID ya existe", value=False)
extensions_text = st.text_input("Extensiones (coma)", value="jpg,jpeg,png,heic,webp")

if st.button("Ingestar carpeta"):
    ext = [item.strip() for item in extensions_text.split(",") if item.strip()]
    try:
        result = api_post(
            "/covers/ingest",
            json={
                "folder": folder,
                "recursive": recursive,
                "overwrite_existing_paths": overwrite_paths,
                "extensions": ext,
            },
        )
        st.success("Ingesta completada")
        st.json(result)
    except Exception as exc:
        st.error(str(exc))

st.divider()

st.info("La orquestacion y la vista de grafo estan en la pagina `Fase 0 - Orquestacion LangGraph`.")

st.subheader("Pendientes de extraccion (detalle)")
try:
    rows = api_get("/movies", params={"stage": "needs_extraction", "limit": 200})
    if rows:
        df = pd.DataFrame(rows)
        st.dataframe(
            df[
                [
                    "id",
                    "image_path",
                    "extraction_title",
                    "workflow_status",
                    "workflow_needs_review",
                    "workflow_review_reason",
                ]
            ]
        )
    else:
        st.info("No hay pendientes.")
except Exception as exc:
    st.error(str(exc))
