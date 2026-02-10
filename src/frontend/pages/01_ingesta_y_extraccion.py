import os

import pandas as pd
import requests
import streamlit as st

try:
    from src.frontend.utils import LONG_TIMEOUT_SECONDS, api_get, api_post, load_stats
except ModuleNotFoundError:  # pragma: no cover
    from frontend.utils import LONG_TIMEOUT_SECONDS, api_get, api_post, load_stats

st.title("Fase 1 - Ingesta y extraccion")

stats = load_stats()
c1, c2, c3, c4, c5, c6 = st.columns(6)
c1.metric("Total", stats["total"])
c2.metric("Sin extraccion", stats["needs_extraction"])
c3.metric("Sin revision", stats["needs_manual_review"])
c4.metric("Sin IMDb", stats["needs_imdb"])
c5.metric("Sin OMDb", stats["needs_omdb"])
c6.metric("Sin traduccion", stats["needs_translation"])

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

st.subheader("Extraccion titulo + equipo")
movie_id = st.text_input("ID concreto (opcional)", value="")
limit = st.number_input("Limite batch", min_value=1, max_value=5000, value=5, step=1)
overwrite = st.checkbox("Reextraer aunque ya haya datos", value=False)
title_model = st.text_input("Modelo para titulo", value=os.getenv("VISION_TITLE_MODEL", "gemma3:27b-it-qat"))
team_model = st.text_input("Modelo para equipo", value=os.getenv("VISION_TEAM_MODEL", "qwen3-vl:32b"))

st.caption(
    f"Esta accion puede tardar varios minutos por item con modelos grandes. "
    f"Timeout actual para esta llamada: {int(LONG_TIMEOUT_SECONDS)}s."
)

if st.button("Ejecutar extraccion"):
    payload = {
        "movie_id": movie_id or None,
        "limit": int(limit),
        "overwrite": overwrite,
        "title_model": title_model,
        "team_model": team_model,
    }
    try:
        result = api_post("/extract/run", json=payload, timeout=LONG_TIMEOUT_SECONDS)
        st.success("Extraccion finalizada")
        st.json(result)
    except requests.exceptions.ReadTimeout:
        st.error(
            "Timeout esperando al backend. "
            "Reduce el limite batch o aumenta API_LONG_TIMEOUT_SECONDS en tu .env y reinicia Streamlit."
        )
    except Exception as exc:
        st.error(str(exc))

st.divider()

st.subheader("Pendientes de extraccion")
try:
    rows = api_get("/movies", params={"stage": "needs_extraction", "limit": 200})
    if rows:
        df = pd.DataFrame(rows)
        st.dataframe(df[["id", "image_path", "extraction_title", "imdb_status", "omdb_status"]])
    else:
        st.info("No hay pendientes.")
except Exception as exc:
    st.error(str(exc))
