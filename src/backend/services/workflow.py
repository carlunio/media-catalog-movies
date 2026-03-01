import importlib.util
from collections import defaultdict
from typing import Any

from ..config import (
    IMDB_MAX_RESULTS,
    TRANSLATION_MODEL,
    VISION_TEAM_MODEL,
    VISION_TITLE_MODEL,
    WORKFLOW_MAX_ATTEMPTS,
)
from . import movies

VALID_STAGES = {"extraction", "imdb", "title_es", "omdb", "translation"}
STAGE_BUCKETS = (
    "extraction",
    "imdb",
    "title_es",
    "omdb",
    "translation",
    "review",
    "done",
    "running",
    "unknown",
)

WORKFLOW_GRAPH_NODES = [
    {"id": "load_movie", "label": "Load movie", "kind": "control"},
    {"id": "apply_action", "label": "Apply action", "kind": "control"},
    {"id": "extract", "label": "Extraction", "kind": "stage", "stage": "extraction"},
    {"id": "imdb", "label": "IMDb search", "kind": "stage", "stage": "imdb"},
    {"id": "title_es", "label": "IMDb title (ES)", "kind": "stage", "stage": "title_es"},
    {"id": "omdb", "label": "OMDb fetch", "kind": "stage", "stage": "omdb"},
    {"id": "translation", "label": "Plot translation", "kind": "stage", "stage": "translation"},
    {"id": "evaluate", "label": "Evaluate", "kind": "control"},
    {"id": "retry", "label": "Retry", "kind": "control"},
    {"id": "end", "label": "End", "kind": "terminal"},
]

WORKFLOW_GRAPH_EDGES = [
    {"source": "load_movie", "target": "apply_action"},
    {"source": "apply_action", "target": "extract"},
    {"source": "extract", "target": "imdb"},
    {"source": "imdb", "target": "title_es"},
    {"source": "title_es", "target": "omdb"},
    {"source": "omdb", "target": "translation"},
    {"source": "translation", "target": "evaluate"},
    {"source": "evaluate", "target": "retry", "label": "route=retry"},
    {"source": "evaluate", "target": "end", "label": "route=end"},
    {"source": "retry", "target": "extract"},
]

WORKFLOW_STAGE_TO_NODE = {
    "extraction": "extract",
    "imdb": "imdb",
    "title_es": "title_es",
    "omdb": "omdb",
    "translation": "translation",
}


def is_langgraph_available() -> bool:
    return importlib.util.find_spec("langgraph") is not None


def _invoke_graph(initial_state: dict[str, Any]) -> dict[str, Any]:
    try:
        from ..workflow import run_workflow_graph
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "langgraph is not installed in the current environment. "
            "Install project dependencies and restart the backend."
        ) from exc

    return run_workflow_graph(initial_state)


def _normalize_stage(value: str | None, *, default: str) -> str:
    if not value:
        return default
    stage = value.strip().lower()
    if stage not in VALID_STAGES:
        raise ValueError(f"Invalid stage: {value}")
    return stage


def graph_definition() -> dict[str, Any]:
    return {
        "langgraph_available": is_langgraph_available(),
        "start_node": "load_movie",
        "end_node": "end",
        "stage_order": ["extraction", "imdb", "title_es", "omdb", "translation"],
        "stage_to_node": WORKFLOW_STAGE_TO_NODE,
        "nodes": WORKFLOW_GRAPH_NODES,
        "edges": WORKFLOW_GRAPH_EDGES,
    }


def _stage_bucket(stage: str | None) -> str:
    normalized = (stage or "").strip().lower()
    if not normalized:
        return "unknown"
    if normalized.startswith("running"):
        return "running"
    if normalized in STAGE_BUCKETS:
        return normalized
    return "unknown"


def snapshot(*, limit: int = 5000, review_limit: int = 200) -> dict[str, Any]:
    rows = movies.list_movies(limit=limit)
    stage_counts: dict[str, int] = defaultdict(int)
    workflow_status_counts: dict[str, int] = defaultdict(int)
    running_nodes: dict[str, int] = defaultdict(int)

    for row in rows:
        stage_counts[_stage_bucket(row.get("pipeline_stage"))] += 1
        status = str(row.get("workflow_status") or "pending").strip().lower() or "pending"
        workflow_status_counts[status] += 1
        if status == "running":
            node = str(row.get("workflow_current_node") or "unknown")
            running_nodes[node] += 1

    for bucket in STAGE_BUCKETS:
        stage_counts.setdefault(bucket, 0)

    review_rows = movies.list_movies(stage="needs_workflow_review", limit=review_limit)
    review_queue = [
        {
            "id": row.get("id"),
            "pipeline_stage": row.get("pipeline_stage"),
            "workflow_current_node": row.get("workflow_current_node"),
            "workflow_review_reason": row.get("workflow_review_reason"),
            "workflow_attempt": row.get("workflow_attempt"),
            "updated_at": row.get("updated_at"),
        }
        for row in review_rows
    ]

    return {
        "total_considered": len(rows),
        "stage_counts": dict(stage_counts),
        "workflow_status_counts": dict(workflow_status_counts),
        "running_nodes": dict(running_nodes),
        "review_queue_size": len(review_rows),
        "review_queue": review_queue,
    }



def run_one(
    movie_id: str,
    *,
    start_stage: str = "extraction",
    stop_after: str | None = None,
    action: str | None = None,
    overwrite: bool = False,
    title_model: str = VISION_TITLE_MODEL,
    team_model: str = VISION_TEAM_MODEL,
    translation_model: str = TRANSLATION_MODEL,
    max_results: int = IMDB_MAX_RESULTS,
    max_attempts: int = WORKFLOW_MAX_ATTEMPTS,
) -> dict[str, Any]:
    stage = _normalize_stage(start_stage, default="extraction")
    stop = _normalize_stage(stop_after, default=stage) if stop_after else None

    if movies.get_movie(movie_id) is None:
        return {"id": movie_id, "status": "error", "error": "Movie not found"}

    result_state = _invoke_graph(
        {
            "movie_id": movie_id,
            "start_stage": stage,
            "stop_after": stop,
            "action": action,
            "overwrite": overwrite,
            "title_model": title_model,
            "team_model": team_model,
            "translation_model": translation_model,
            "max_results": int(max_results),
            "max_attempts": int(max_attempts),
            "stop_pipeline": False,
        }
    )

    movie = movies.get_movie(movie_id)

    if movie is None:
        return {"id": movie_id, "status": "error", "error": "Movie disappeared after workflow run"}

    failed_step = result_state.get("failed_step")
    error = result_state.get("error")

    status = "ok"
    if movie.get("workflow_status") == "review":
        status = "review"
    elif failed_step:
        status = "error"
    elif result_state.get("outcome") == "done":
        status = "done"
    elif result_state.get("outcome") == "approved":
        status = "approved"
    elif result_state.get("outcome") == "partial":
        status = "partial"

    return {
        "id": movie_id,
        "status": status,
        "workflow_status": movie.get("workflow_status"),
        "workflow_current_node": movie.get("workflow_current_node"),
        "workflow_attempt": movie.get("workflow_attempt"),
        "workflow_needs_review": movie.get("workflow_needs_review"),
        "workflow_review_reason": movie.get("workflow_review_reason"),
        "failed_step": failed_step,
        "error": error,
        "imdb_id": movie.get("imdb_id"),
        "imdb_url": movie.get("imdb_url"),
        "imdb_title_es_status": movie.get("imdb_title_es_status"),
        "omdb_status": movie.get("omdb_status"),
        "translation_status": movie.get("translation_status"),
        "outcome": result_state.get("outcome"),
    }



def run_batch(
    *,
    movie_id: str | None = None,
    limit: int = 20,
    start_stage: str = "extraction",
    stop_after: str | None = None,
    action: str | None = None,
    overwrite: bool = False,
    title_model: str = VISION_TITLE_MODEL,
    team_model: str = VISION_TEAM_MODEL,
    translation_model: str = TRANSLATION_MODEL,
    max_results: int = IMDB_MAX_RESULTS,
    max_attempts: int = WORKFLOW_MAX_ATTEMPTS,
) -> dict[str, Any]:
    stage = _normalize_stage(start_stage, default="extraction")
    stop = _normalize_stage(stop_after, default=stage) if stop_after else None

    if movie_id:
        targets = [movie_id]
    else:
        targets = movies.movie_ids_for_workflow(
            limit=limit,
            start_stage=stage,
            overwrite=overwrite,
        )

    items: list[dict[str, Any]] = []
    for target_id in targets:
        items.append(
            run_one(
                target_id,
                start_stage=stage,
                stop_after=stop,
                action=action,
                overwrite=overwrite,
                title_model=title_model,
                team_model=team_model,
                translation_model=translation_model,
                max_results=max_results,
                max_attempts=max_attempts,
            )
        )

    return {
        "requested": len(targets),
        "processed": len(items),
        "items": items,
    }



def review_action(
    movie_id: str,
    *,
    action: str,
    max_attempts: int = WORKFLOW_MAX_ATTEMPTS,
    title_model: str = VISION_TITLE_MODEL,
    team_model: str = VISION_TEAM_MODEL,
    translation_model: str = TRANSLATION_MODEL,
    max_results: int = IMDB_MAX_RESULTS,
) -> dict[str, Any]:
    action_to_stage = {
        "approve": "translation",
        "retry_from_extraction": "extraction",
        "retry_from_imdb": "imdb",
        "retry_from_title_es": "title_es",
        "retry_from_omdb": "omdb",
        "retry_from_translation": "translation",
    }

    normalized = action.strip().lower()
    stage = action_to_stage.get(normalized)
    if stage is None:
        raise ValueError(f"Invalid review action: {action}")

    return run_one(
        movie_id,
        start_stage=stage,
        stop_after=None,
        action=normalized,
        overwrite=True,
        title_model=title_model,
        team_model=team_model,
        translation_model=translation_model,
        max_results=max_results,
        max_attempts=max_attempts,
    )


def mark_review(movie_id: str, *, reason: str | None = None, node: str = "manual") -> dict[str, Any]:
    movie = movies.get_movie(movie_id)
    if movie is None:
        raise ValueError("Movie not found")

    text = (reason or "").strip()
    if not text:
        text = "Marked for manual review from Streamlit orchestration page"

    movies.set_workflow_review(movie_id, node=node.strip() or "manual", reason=text, error=None)
    updated = movies.get_movie(movie_id)
    return {
        "id": movie_id,
        "status": "review",
        "workflow_status": updated.get("workflow_status") if updated else None,
        "workflow_needs_review": updated.get("workflow_needs_review") if updated else True,
        "workflow_review_reason": updated.get("workflow_review_reason") if updated else text,
        "pipeline_stage": updated.get("pipeline_stage") if updated else "review",
    }
