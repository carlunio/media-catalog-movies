import os

import pandas as pd
import requests
import streamlit as st

try:
    from src.frontend.utils import (
        LONG_TIMEOUT_SECONDS,
        api_get,
        api_post,
        render_timeout_controls,
        select_ollama_model,
    )
except ModuleNotFoundError:  # pragma: no cover
    from frontend.utils import (
        LONG_TIMEOUT_SECONDS,
        api_get,
        api_post,
        render_timeout_controls,
        select_ollama_model,
    )

st.title("Fase 0 - Orquestacion LangGraph")
render_timeout_controls()


def _dot_escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


def _build_workflow_dot(definition: dict) -> str:
    nodes = definition.get("nodes", [])
    edges = definition.get("edges", [])

    stage_colors = {
        "extraction": "#d9f2ff",
        "imdb": "#e2f7dc",
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


st.subheader("Grafo")
graph_def: dict = {}
try:
    graph_def = api_get("/workflow/graph")
    langgraph_available = bool(graph_def.get("langgraph_available"))
    if langgraph_available:
        st.success("LangGraph disponible en backend")
    else:
        st.warning("LangGraph no esta instalado en este entorno backend")

    dot_graph = _build_workflow_dot(graph_def)
    try:
        st.graphviz_chart(dot_graph, use_container_width=True)
    except Exception:
        st.code(dot_graph, language="dot")
except Exception as exc:
    st.error(str(exc))
    st.info("No se pudo cargar el grafo de workflow.")

st.divider()

st.subheader("Estado global")
snapshot_limit = st.number_input(
    "Peliculas a considerar en resumen",
    min_value=1,
    max_value=50000,
    value=5000,
    step=100,
)
review_limit = st.number_input(
    "Tamano maximo de cola review",
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
c1, c2, c3, c4, c5, c6, c7 = st.columns(7)
c1.metric("Extraccion", stage_counts.get("extraction", 0))
c2.metric("IMDb", stage_counts.get("imdb", 0))
c3.metric("OMDb", stage_counts.get("omdb", 0))
c4.metric("Traduccion", stage_counts.get("translation", 0))
c5.metric("Review", stage_counts.get("review", 0))
c6.metric("Done", stage_counts.get("done", 0))
c7.metric("Running", stage_counts.get("running", 0))

with st.expander("Detalle de estado de workflow", expanded=False):
    col_status, col_running = st.columns(2)
    with col_status:
        status_counts = snapshot.get("workflow_status_counts", {})
        if status_counts:
            df_status = pd.DataFrame(
                [{"workflow_status": key, "count": value} for key, value in status_counts.items()]
            ).sort_values("workflow_status")
            st.dataframe(df_status, use_container_width=True)
        else:
            st.info("Sin datos de workflow_status")

    with col_running:
        running_nodes = snapshot.get("running_nodes", {})
        if running_nodes:
            df_running = pd.DataFrame(
                [{"node": key, "count": value} for key, value in running_nodes.items()]
            ).sort_values("count", ascending=False)
            st.dataframe(df_running, use_container_width=True)
        else:
            st.info("No hay nodos en ejecucion")

st.divider()

st.subheader("Ejecutar workflow")
run_movie_id = st.text_input("ID concreto (opcional)", value="", key="orq_run_movie_id")
run_limit = st.number_input(
    "Limite batch",
    min_value=1,
    max_value=5000,
    value=20,
    step=1,
    key="orq_run_limit",
)
start_stage = st.selectbox("Nodo inicial", ["extraction", "imdb", "omdb", "translation"], index=0)
stop_selection = st.selectbox("Parar despues de", ["full", "extraction", "imdb", "omdb", "translation"], index=0)
stop_after = None if stop_selection == "full" else stop_selection
overwrite = st.checkbox("Sobrescribir/rehacer nodos", value=False, key="orq_run_overwrite")
max_results = st.number_input("Resultados maximos IMDb", min_value=1, max_value=30, value=10, step=1)
max_attempts = st.number_input("Intentos maximos workflow", min_value=0, max_value=20, value=2, step=1)

title_model = select_ollama_model(
    "Modelo para titulo",
    os.getenv("VISION_TITLE_MODEL", "gemma3:27b-it-qat"),
    key="orq_title_model",
)
team_model = select_ollama_model(
    "Modelo para equipo",
    os.getenv("VISION_TEAM_MODEL", "qwen3-vl:32b"),
    key="orq_team_model",
)
translation_model = select_ollama_model(
    "Modelo para traduccion",
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
        st.error("Timeout esperando al backend. Reduce limite o cambia el modo en Sidebar > HTTP timeout.")
    except Exception as exc:
        st.error(str(exc))

st.divider()

st.subheader("Cola de revision")
review_queue = snapshot.get("review_queue", [])
st.caption(f"Peliculas en cola de review: {snapshot.get('review_queue_size', 0)}")

if review_queue:
    df_review = pd.DataFrame(review_queue)
    show_review_cols = [
        "id",
        "pipeline_stage",
        "workflow_current_node",
        "workflow_review_reason",
        "workflow_attempt",
        "updated_at",
    ]
    st.dataframe(df_review[show_review_cols], use_container_width=True)
    default_target = str(df_review.iloc[0]["id"])
else:
    st.info("No hay peliculas pendientes de review.")
    default_target = ""

target_id = st.text_input("ID para accion de review", value=default_target)
review_reason = st.text_input("Motivo para marcar review manual", value="")

action_col1, action_col2 = st.columns(2)
with action_col1:
    if st.button("Marcar review manual"):
        if not target_id.strip():
            st.error("Indica un ID")
        else:
            try:
                result = api_post(
                    f"/workflow/review/{target_id.strip()}/mark",
                    json={"reason": review_reason or None, "node": "manual"},
                    timeout=LONG_TIMEOUT_SECONDS,
                )
                st.success("Marcado en review")
                st.json(result)
            except requests.exceptions.ReadTimeout:
                st.error("Timeout marcando review")
            except Exception as exc:
                st.error(str(exc))

with action_col2:
    review_action = st.selectbox(
        "Accion para el ID",
        [
            "approve",
            "retry_from_extraction",
            "retry_from_imdb",
            "retry_from_omdb",
            "retry_from_translation",
        ],
        index=0,
    )
    if st.button("Ejecutar accion de review"):
        if not target_id.strip():
            st.error("Indica un ID")
        else:
            try:
                result = api_post(
                    f"/workflow/review/{target_id.strip()}",
                    json={"action": review_action, "max_attempts": int(max_attempts)},
                    timeout=LONG_TIMEOUT_SECONDS,
                )
                st.success("Accion ejecutada")
                st.json(result)
            except requests.exceptions.ReadTimeout:
                st.error("Timeout ejecutando accion")
            except Exception as exc:
                st.error(str(exc))

st.divider()

st.subheader("Listado por etapa")
stage_options = {
    "Todas": None,
    "Extraccion": "pipeline_extraction",
    "IMDb": "pipeline_imdb",
    "OMDb": "pipeline_omdb",
    "Traduccion": "pipeline_translation",
    "Review": "pipeline_review",
    "Done": "pipeline_done",
}
selected_stage_label = st.selectbox("Filtrar por etapa", list(stage_options.keys()), index=0)
selected_stage = stage_options[selected_stage_label]
list_limit = st.number_input("Limite listado", min_value=1, max_value=50000, value=200, step=50)

try:
    params = {"limit": int(list_limit)}
    if selected_stage:
        params["stage"] = selected_stage
    rows = api_get("/movies", params=params)
    if rows:
        df = pd.DataFrame(rows)
        cols = [
            "id",
            "pipeline_stage",
            "workflow_status",
            "workflow_current_node",
            "workflow_needs_review",
            "workflow_review_reason",
            "imdb_status",
            "omdb_status",
            "translation_status",
            "updated_at",
        ]
        present_cols = [col for col in cols if col in df.columns]
        st.dataframe(df[present_cols], use_container_width=True)
    else:
        st.info("No hay peliculas en este filtro.")
except Exception as exc:
    st.error(str(exc))
