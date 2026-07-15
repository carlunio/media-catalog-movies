import os

import pandas as pd
import streamlit as st

try:
    from src.frontend.utils import api_get, api_post, configure_page, load_stats, render_icon_heading, render_timeout_controls
except ModuleNotFoundError:  # pragma: no cover
    from frontend.utils import api_get, api_post, configure_page, load_stats, render_icon_heading, render_timeout_controls

configure_page()
render_icon_heading("Fase 1 - Lectura", icon="images", level=1)
render_timeout_controls()

stats = load_stats()
c1, c2, c3, c4, c5, c6, c7, c8 = st.columns(8)
c1.metric("Total", stats.get("total", 0))
c2.metric("Sin extracción", stats.get("needs_extraction", 0))
c3.metric("Sin revisión", stats.get("needs_manual_review", 0))
c4.metric("Sin IMDb", stats.get("needs_imdb", 0))
c5.metric("Sin título ES IMDb", stats.get("needs_title_es", 0))
c6.metric("Sin OMDb", stats.get("needs_omdb", 0))
c7.metric("Sin traducción", stats.get("needs_translation", 0))
c8.metric("Pendiente de revisión", stats.get("needs_workflow_review", 0))

st.divider()

render_icon_heading("Leer carátulas", icon="folder-open", level=2)
default_folder = os.getenv("COVERS_DIR", "data/input")
folder = st.text_input("Carpeta origen", value=default_folder)
ing_c1, ing_c2, ing_c3 = st.columns([1, 1, 2])
with ing_c1:
    recursive = st.checkbox("Recursivo", value=True)
with ing_c2:
    overwrite_paths = st.checkbox("Sobrescribir rutas", value=False)
with ing_c3:
    extensions_text = st.text_input("Extensiones (coma)", value="jpg,jpeg,png,heic,webp")

if st.button("Leer carpeta"):
    ext = [item.strip() for item in extensions_text.split(",") if item.strip()]
    try:
        result = api_post(
            "/covers/read",
            json={
                "folder": folder,
                "recursive": recursive,
                "overwrite_existing_paths": overwrite_paths,
                "extensions": ext,
            },
        )
        st.success("Lectura completada")
        st.json(result)
    except Exception as exc:
        st.error(str(exc))

render_icon_heading("Reconciliar rutas locales", icon="link", level=2)
rebind_folder = st.text_input("Carpeta local para reconciliar", value=default_folder)
rb_c1, rb_c2, rb_c3 = st.columns([1, 1, 1.5])
with rb_c1:
    rebind_recursive = st.checkbox("Buscar en subcarpetas", value=True, key="rebind_recursive")
with rb_c2:
    rebind_only_missing = st.checkbox(
        "Solo rutas rotas",
        value=True,
        key="rebind_only_missing",
    )
with rb_c3:
    rebind_limit = st.number_input(
        "Límite de películas",
        min_value=1,
        max_value=200000,
        value=50000,
        step=1000,
        key="rebind_limit",
    )

if st.button("Reconciliar rutas"):
    ext = [item.strip() for item in extensions_text.split(",") if item.strip()]
    try:
        result = api_post(
            "/covers/rebind",
            json={
                "folder": rebind_folder or None,
                "recursive": bool(rebind_recursive),
                "extensions": ext,
                "limit": int(rebind_limit),
                "only_missing": bool(rebind_only_missing),
            },
        )
        st.success("Reconciliación completada")
        st.json(result)
    except Exception as exc:
        st.error(str(exc))

st.divider()

st.info("La orquestación y la vista de grafo están en la página `Fase 0 - Orquestación LangGraph`.")

render_icon_heading("Pendientes de extracción (detalle)", icon="list", level=2)
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
