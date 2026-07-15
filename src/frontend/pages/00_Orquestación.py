import os

import pandas as pd
import requests
import streamlit as st

try:
    from src.frontend.utils import (
        LONG_TIMEOUT_SECONDS,
        api_get,
        api_post,
        infer_review_stage,
        node_ui_label,
        configure_page,
        render_icon_heading,
        render_timeout_controls,
        select_movie_id,
        select_ollama_model,
        stage_ui_label,
        set_selected_movie_id,
    )
except ModuleNotFoundError:  # pragma: no cover
    from frontend.utils import (
        LONG_TIMEOUT_SECONDS,
        api_get,
        api_post,
        infer_review_stage,
        node_ui_label,
        configure_page,
        render_icon_heading,
        render_timeout_controls,
        select_movie_id,
        select_ollama_model,
        stage_ui_label,
        set_selected_movie_id,
    )

configure_page()
render_icon_heading("Fase 0 - Orquestación LangGraph", icon="sitemap", level=1)
render_timeout_controls()


def _dot_escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


def _build_workflow_dot(definition: dict) -> str:
    nodes = definition.get("nodes", [])
    edges = definition.get("edges", [])

    stage_colors = {
        "extraction": "#d9f2ff",
        "imdb": "#e2f7dc",
        "title_es": "#dff0e2",
        "omdb": "#fff4cf",
        "translation": "#f8def8",
    }

    lines = [
        "digraph Workflow {",
        "  rankdir=LR;",
        '  node [shape=box style="rounded,filled" fontname="Helvetica" fontsize=11];',
        '  edge [fontname="Helvetica" fontsize=10];',
    ]

    for node in nodes:
        node_id = str(node.get("id", ""))
        if not node_id:
            continue

        label = _dot_escape(str(node.get("label") or node_id))
        kind = str(node.get("kind") or "control")
        stage = str(node.get("stage") or "")

        shape = "box"
        fill = "#f2f2f2"
        if kind == "control":
            shape = "ellipse"
            fill = "#ebebeb"
        elif kind == "stage":
            fill = stage_colors.get(stage, "#efefef")
        elif kind == "terminal":
            shape = "doublecircle"
            fill = "#d5ecd3"

        lines.append(
            f'  "{_dot_escape(node_id)}" [label="{label}" shape={shape} fillcolor="{fill}"];'
        )

    for edge in edges:
        source = str(edge.get("source") or "")
        target = str(edge.get("target") or "")
        if not source or not target:
            continue

        label = str(edge.get("label") or "").strip()
        if label:
            lines.append(
                f'  "{_dot_escape(source)}" -> "{_dot_escape(target)}" '
                f'[label="{_dot_escape(label)}"];'
            )
        else:
            lines.append(f'  "{_dot_escape(source)}" -> "{_dot_escape(target)}";')

    lines.append("}")
    return "\n".join(lines)


def _switch_page(target: str) -> None:
    switch_fn = getattr(st, "switch_page", None)
    if callable(switch_fn):
        switch_fn(target)
    else:
        st.info("Tu versión de Streamlit no soporta `switch_page`.")


render_icon_heading("Grafo", icon="sitemap", level=2)
graph_def: dict = {}
try:
    graph_def = api_get("/workflow/graph")
    langgraph_available = bool(graph_def.get("langgraph_available"))
    if langgraph_available:
        st.success("LangGraph disponible en backend")
    else:
        st.warning("LangGraph no está instalado en este entorno backend")

    dot_graph = _build_workflow_dot(graph_def)
    try:
        st.graphviz_chart(dot_graph, width="stretch")
    except Exception:
        st.code(dot_graph, language="dot")
except Exception as exc:
    st.error(str(exc))
    st.info("No se pudo cargar el grafo de workflow.")

st.divider()

render_icon_heading("Estado global", icon="chart-line", level=2)
state_c1, state_c2 = st.columns(2)
with state_c1:
    snapshot_limit = st.number_input(
        "Películas a considerar en resumen",
        min_value=1,
        max_value=50000,
        value=5000,
        step=100,
    )
with state_c2:
    review_limit = st.number_input(
        "Tamaño máximo de cola de revisión",
        min_value=1,
        max_value=5000,
        value=200,
        step=10,
    )

snapshot = {
    "stage_counts": {},
    "workflow_status_counts": {},
    "running_nodes": {},
    "review_queue": [],
    "review_queue_size": 0,
}
try:
    snapshot = api_get(
        "/workflow/snapshot",
        params={"limit": int(snapshot_limit), "review_limit": int(review_limit)},
    )
except Exception as exc:
    st.error(str(exc))
    st.info("No se pudo cargar el snapshot de workflow.")

stage_counts = snapshot.get("stage_counts", {})
c1, c2, c3, c4, c5, c6, c7, c8 = st.columns(8)
c1.metric("Extracción", stage_counts.get("extraction", 0))
c2.metric("IMDb", stage_counts.get("imdb", 0))
c3.metric("IMDb Título ES", stage_counts.get("title_es", 0))
c4.metric("OMDb", stage_counts.get("omdb", 0))
c5.metric("Traducción", stage_counts.get("translation", 0))
c6.metric("Revisión", stage_counts.get("review", 0))
c7.metric("Done", stage_counts.get("done", 0))
c8.metric("Running", stage_counts.get("running", 0))

with st.expander("Detalle de estado de workflow", expanded=False):
    col_status, col_running = st.columns(2)
    with col_status:
        status_counts = snapshot.get("workflow_status_counts", {})
        if status_counts:
            df_status = pd.DataFrame(
                [{"workflow_status": key, "count": value} for key, value in status_counts.items()]
            ).sort_values("workflow_status")
            st.dataframe(df_status, width="stretch")
        else:
            st.info("Sin datos de workflow_status")

    with col_running:
        running_nodes = snapshot.get("running_nodes", {})
        if running_nodes:
            df_running = pd.DataFrame(
                [{"node": node_ui_label(key), "count": value} for key, value in running_nodes.items()]
            ).sort_values("count", ascending=False)
            st.dataframe(df_running, width="stretch")
        else:
            st.info("No hay nodos en ejecución")

st.divider()

render_icon_heading("Ejecutar workflow", icon="play", level=2)
run_c1, run_c2 = st.columns([2, 1])
with run_c1:
    run_movie_id = st.text_input("ID concreto (opcional)", value="", key="orq_run_movie_id")
with run_c2:
    run_limit = st.number_input(
        "Límite batch",
        min_value=1,
        max_value=5000,
        value=20,
        step=1,
        key="orq_run_limit",
    )

run_c3, run_c4, run_c5 = st.columns([1, 1, 1])
stage_options = [
    ("Extracción", "extraction"),
    ("IMDb", "imdb"),
    ("IMDb Título ES", "title_es"),
    ("OMDb", "omdb"),
    ("Traducción", "translation"),
]
stage_label_to_key = {label: key for label, key in stage_options}
with run_c3:
    start_stage_label = st.selectbox(
        "Nodo inicial",
        [label for label, _ in stage_options],
        index=0,
    )
with run_c4:
    stop_selection_label = st.selectbox(
        "Parar despues de",
        ["Todo workflow"] + [label for label, _ in stage_options],
        index=0,
    )
with run_c5:
    overwrite = st.checkbox("Sobrescribir nodos", value=False, key="orq_run_overwrite")

start_stage = stage_label_to_key[start_stage_label]
stop_after = None if stop_selection_label == "Todo workflow" else stage_label_to_key[stop_selection_label]

run_c6, run_c7 = st.columns(2)
with run_c6:
    max_results = st.number_input("Resultados máximos IMDb", min_value=1, max_value=30, value=10, step=1)
with run_c7:
    max_attempts = st.number_input("Intentos máximos workflow", min_value=0, max_value=20, value=2, step=1)

model_c1, model_c2, model_c3 = st.columns(3)
with model_c1:
    title_model = select_ollama_model(
        "Modelo para título",
        os.getenv("VISION_TITLE_MODEL", "gemma3:27b-it-qat"),
        key="orq_title_model",
    )
with model_c2:
    team_model = select_ollama_model(
        "Modelo para equipo",
        os.getenv("VISION_TEAM_MODEL", "qwen3-vl:32b"),
        key="orq_team_model",
    )
with model_c3:
    translation_model = select_ollama_model(
        "Modelo para traducción",
        os.getenv("TRANSLATION_MODEL", "phi4:latest"),
        key="orq_translation_model",
    )

if st.button("Ejecutar workflow batch"):
    payload = {
        "movie_id": run_movie_id or None,
        "limit": int(run_limit),
        "start_stage": start_stage,
        "stop_after": stop_after,
        "overwrite": overwrite,
        "title_model": title_model,
        "team_model": team_model,
        "translation_model": translation_model,
        "max_results": int(max_results),
        "max_attempts": int(max_attempts),
    }
    try:
        result = api_post("/workflow/run", json=payload, timeout=LONG_TIMEOUT_SECONDS)
        st.success("Workflow completado")
        st.json(result)
    except requests.exceptions.ReadTimeout:
        st.error("Timeout esperando al backend. Reduce límite o cambia el modo en Sidebar > HTTP timeout.")
    except Exception as exc:
        st.error(str(exc))

st.divider()

render_icon_heading("Cola de revisión", icon="clipboard-check", level=2)
review_queue = snapshot.get("review_queue", [])
st.caption(f"Películas en cola de revisión: {snapshot.get('review_queue_size', 0)}")

review_rows_detailed: list[dict] = []
try:
    review_rows_detailed = api_get(
        "/movies",
        params={"stage": "needs_workflow_review", "limit": int(review_limit)},
    )
except Exception:
    review_rows_detailed = []

if review_queue:
    df_review = pd.DataFrame(review_queue)
    if not df_review.empty:
        df_review["review_stage"] = [
            stage_ui_label(infer_review_stage(row) or "unknown")
            for row in df_review.to_dict(orient="records")
        ]
        df_review["pipeline_stage"] = [stage_ui_label(value) for value in df_review["pipeline_stage"]]
        df_review["workflow_current_node"] = [node_ui_label(value) for value in df_review["workflow_current_node"]]
    show_review_cols = [
        "id",
        "review_stage",
        "pipeline_stage",
        "workflow_current_node",
        "workflow_review_reason",
        "workflow_attempt",
        "updated_at",
    ]
    st.dataframe(df_review[show_review_cols], width="stretch")
    default_target = str(df_review.iloc[0]["id"])
else:
    st.info("No hay películas pendientes de revisión.")
    default_target = ""

selected_review_id = ""
selected_review_stage = None
if review_rows_detailed:
    st.markdown("#### Búsqueda rápida en revisión")
    selected_review_id = select_movie_id(
        review_rows_detailed,
        label="Buscar película en revisión",
        key="orq_review_selector",
    )
    set_selected_movie_id(selected_review_id)
    selected_row = next((row for row in review_rows_detailed if row.get("id") == selected_review_id), None)
    if selected_row:
        selected_review_stage = infer_review_stage(selected_row)
        selected_review_stage_label = stage_ui_label(selected_review_stage or "unknown")
        st.caption(
            f"Seleccionada `{selected_review_id}` | "
            f"fase de fallo detectada: `{selected_review_stage_label}`"
        )

        nav_f2, nav_fail, nav_all = st.columns(3)
        with nav_f2:
            if st.button("Abrir Fase 2 - Título", width="stretch", key="orq_to_f2"):
                _switch_page("pages/02_Título.py")
        with nav_fail:
            if st.button("Abrir fase del fallo", width="stretch", key="orq_to_fail"):
                page_map = {
                    "extraction": "pages/02_Título.py",
                    "imdb": "pages/03_IMDb.py",
                    "title_es": "pages/03_IMDb.py",
                    "omdb": "pages/04_OMDb.py",
                    "translation": "pages/05_Sinopsis_ES.py",
                }
                page = page_map.get(selected_review_stage or "", "pages/02_Título.py")
                _switch_page(page)
        with nav_all:
            if st.button("Abrir Orquestación", width="stretch", key="orq_to_orq"):
                _switch_page("pages/00_Orquestación.py")

if selected_review_id:
    default_target = selected_review_id or default_target

target_id = default_target
if selected_review_id:
    st.text_input("ID para acción de revisión", value=target_id, disabled=True)
    st.caption("La acción se ejecuta sobre la película seleccionada en la cola de revisión.")
else:
    target_id = st.text_input("ID para acción de revisión", value=default_target)
review_reason = st.text_input("Motivo para marcar revisión manual", value="")

action_col1, action_col2 = st.columns(2)
with action_col1:
    if st.button("Marcar revisión manual"):
        if not target_id.strip():
            st.error("Indica un ID")
        else:
            try:
                result = api_post(
                    f"/workflow/review/{target_id.strip()}/mark",
                    json={"reason": review_reason or None, "node": "manual"},
                    timeout=LONG_TIMEOUT_SECONDS,
                )
                st.success("Marcado en revisión")
                st.json(result)
            except requests.exceptions.ReadTimeout:
                st.error("Timeout marcando revisión")
            except Exception as exc:
                st.error(str(exc))

with action_col2:
    action_label = "Acción para la película seleccionada" if selected_review_id else "Acción para el ID"
    review_action_options = [
        ("Aprobar", "approve"),
        ("Reejecutar desde extracción", "retry_from_extraction"),
        ("Reejecutar desde IMDb", "retry_from_imdb"),
        ("Reejecutar desde IMDb título ES", "retry_from_title_es"),
        ("Reejecutar desde OMDb", "retry_from_omdb"),
        ("Reejecutar desde traducción", "retry_from_translation"),
    ]
    action_label_to_key = {label: key for label, key in review_action_options}
    review_action = st.selectbox(
        action_label,
        [label for label, _ in review_action_options],
        index=0,
    )
    if st.button("Ejecutar acción de revisión"):
        if not target_id.strip():
            st.error("Indica un ID")
        else:
            try:
                result = api_post(
                    f"/workflow/review/{target_id.strip()}",
                    json={
                        "action": action_label_to_key[review_action],
                        "max_attempts": int(max_attempts),
                        "title_model": title_model,
                        "team_model": team_model,
                        "translation_model": translation_model,
                        "max_results": int(max_results),
                    },
                    timeout=LONG_TIMEOUT_SECONDS,
                )
                st.success("Acción ejecutada")
                st.json(result)
            except requests.exceptions.ReadTimeout:
                st.error("Timeout ejecutando acción")
            except Exception as exc:
                st.error(str(exc))

st.divider()

render_icon_heading("Listado por etapa", icon="table", level=2)
stage_options = {
    "Todas": None,
    "Extracción": "pipeline_extraction",
    "IMDb": "pipeline_imdb",
    "IMDb Título ES": "pipeline_title_es",
    "OMDb": "pipeline_omdb",
    "Traducción": "pipeline_translation",
    "Revisión": "pipeline_review",
    "Done": "pipeline_done",
}
list_c1, list_c2 = st.columns([2, 1])
with list_c1:
    selected_stage_label = st.selectbox("Filtrar por etapa", list(stage_options.keys()), index=0)
with list_c2:
    list_limit = st.number_input("Límite listado", min_value=1, max_value=50000, value=200, step=50)
selected_stage = stage_options[selected_stage_label]

try:
    params = {"limit": int(list_limit)}
    if selected_stage:
        params["stage"] = selected_stage
    rows = api_get("/movies", params=params)
    if rows:
        df = pd.DataFrame(rows)
        if "pipeline_stage" in df.columns:
            df["pipeline_stage_ui"] = [stage_ui_label(value) for value in df["pipeline_stage"]]
        if "workflow_current_node" in df.columns:
            df["workflow_current_node_ui"] = [node_ui_label(value) for value in df["workflow_current_node"]]
        cols = [
            "id",
            "pipeline_stage_ui",
            "workflow_status",
            "workflow_current_node_ui",
            "workflow_needs_review",
            "workflow_review_reason",
            "imdb_status",
            "imdb_title_es_status",
            "omdb_status",
            "translation_status",
            "updated_at",
        ]
        present_cols = [col for col in cols if col in df.columns]
        st.dataframe(
            df[present_cols].rename(
                columns={
                    "pipeline_stage_ui": "pipeline_stage",
                    "workflow_current_node_ui": "workflow_current_node",
                }
            ),
            width="stretch",
        )
    else:
        st.info("No hay películas en este filtro.")
except Exception as exc:
    st.error(str(exc))
