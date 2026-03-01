from typing import Any, Literal, TypedDict

from langgraph.graph import END, StateGraph

from ..config import (
    IMDB_MAX_RESULTS,
    TRANSLATION_MODEL,
    VISION_TEAM_MODEL,
    VISION_TITLE_MODEL,
    WORKFLOW_MAX_ATTEMPTS,
)
from ..multi_value import PLOT_MULTI_SEPARATOR, split_values
from ..services import cover_extraction, imdb_links, imdb_title_es, movies, omdb_data, plot_translation

StageName = Literal["extraction", "imdb", "title_es", "omdb", "translation"]

STAGE_ORDER: dict[StageName, int] = {
    "extraction": 1,
    "imdb": 2,
    "title_es": 3,
    "omdb": 4,
    "translation": 5,
}

RETRY_STAGE_MAP: dict[str, StageName] = {
    "extraction": "extraction",
    "imdb": "imdb",
    "title_es": "title_es",
    "omdb": "imdb",
    "translation": "translation",
}


class WorkflowState(TypedDict, total=False):
    movie_id: str
    movie: dict[str, Any] | None

    start_stage: StageName
    stop_after: StageName | None
    action: str | None
    overwrite: bool

    title_model: str
    team_model: str
    translation_model: str
    max_results: int
    max_attempts: int

    attempt: int
    failed_step: str | None
    error: str | None

    stop_pipeline: bool
    outcome: str
    route: Literal["retry", "end"]


_GRAPH = None


def _stage_enabled(state: WorkflowState, stage: StageName) -> bool:
    start_stage = state.get("start_stage", "extraction")
    start_idx = STAGE_ORDER.get(start_stage, 1)
    return STAGE_ORDER[stage] >= start_idx



def _should_stop_after(state: WorkflowState, stage: StageName) -> bool:
    return state.get("stop_after") == stage



def _with_failure(movie_id: str, *, step: str, error: str) -> WorkflowState:
    movies.set_workflow_error(movie_id, node=step, error=error)
    return {
        "failed_step": step,
        "error": error,
    }



def _load_movie_node(state: WorkflowState) -> WorkflowState:
    movie_id = state["movie_id"]
    movie = movies.get_movie(movie_id)

    if movie is None:
        return {
            "failed_step": "load_movie",
            "error": f"Movie not found: {movie_id}",
            "stop_pipeline": True,
            "route": "end",
        }

    movies.set_workflow_running(movie_id, node="load_movie", action=state.get("action"))
    return {
        "movie": movie,
        "attempt": int(movie.get("workflow_attempt") or 0),
    }



def _apply_action_node(state: WorkflowState) -> WorkflowState:
    if state.get("failed_step"):
        return {}

    movie_id = state["movie_id"]
    action = (state.get("action") or "").strip().lower()

    if not action or action == "none":
        return {}

    movies.set_workflow_running(movie_id, node="apply_action", action=action)

    if action == "approve":
        movies.clear_workflow_review(movie_id)
        movies.set_workflow_done(movie_id, node="review_approved", action=action)
        return {
            "stop_pipeline": True,
            "outcome": "approved",
        }

    retry_action_to_stage: dict[str, StageName] = {
        "retry_from_extraction": "extraction",
        "retry_from_imdb": "imdb",
        "retry_from_title_es": "title_es",
        "retry_from_omdb": "omdb",
        "retry_from_translation": "translation",
    }

    retry_stage = retry_action_to_stage.get(action)
    if retry_stage is None:
        return _with_failure(movie_id, step="apply_action", error=f"Unsupported action: {action}")

    attempt = movies.increment_workflow_attempt(movie_id)
    movies.reset_from_stage(movie_id, retry_stage)
    movies.clear_workflow_review(movie_id)

    refreshed = movies.get_movie(movie_id)
    return {
        "attempt": attempt,
        "movie": refreshed,
        "start_stage": retry_stage,
        "overwrite": True,
        "failed_step": None,
        "error": None,
        "stop_pipeline": False,
        "outcome": "retry",
    }



def _extract_node(state: WorkflowState) -> WorkflowState:
    if state.get("failed_step") or state.get("stop_pipeline"):
        return {}

    if not _stage_enabled(state, "extraction"):
        return {}

    movie_id = state["movie_id"]
    movies.set_workflow_running(movie_id, node="extract_title_team", action=state.get("action"))

    movie = movies.get_movie(movie_id)
    if movie is None:
        return _with_failure(movie_id, step="extraction", error="Movie disappeared during extraction")

    should_run = bool(state.get("overwrite")) or not movie.get("extraction_title") or not movie.get("extraction_team")
    if should_run:
        try:
            payload = cover_extraction.extract_from_cover(
                movie["image_path"],
                title_model=state.get("title_model", VISION_TITLE_MODEL),
                team_model=state.get("team_model", VISION_TEAM_MODEL),
            )
            movies.update_extraction(
                movie_id,
                title=payload["title"],
                team=payload["team"],
                title_raw=payload["title_raw"],
                team_raw=payload["team_raw"],
            )
        except Exception as exc:
            return _with_failure(movie_id, step="extraction", error=str(exc))

    refreshed = movies.get_movie(movie_id)
    if _should_stop_after(state, "extraction"):
        return {
            "movie": refreshed,
            "stop_pipeline": True,
            "outcome": "stopped_after_extraction",
        }

    return {"movie": refreshed}



def _imdb_node(state: WorkflowState) -> WorkflowState:
    if state.get("failed_step") or state.get("stop_pipeline"):
        return {}

    if not _stage_enabled(state, "imdb"):
        return {}

    movie_id = state["movie_id"]
    movies.set_workflow_running(movie_id, node="search_imdb", action=state.get("action"))

    movie = movies.get_movie(movie_id)
    if movie is None:
        return _with_failure(movie_id, step="imdb", error="Movie disappeared during IMDb search")

    effective_title = str(movie.get("manual_title") or movie.get("extraction_title") or "")
    title_parts = split_values(effective_title)
    imdb_url_parts = split_values(movie.get("imdb_url"))
    imdb_incomplete = len(title_parts) > 1 and len(imdb_url_parts) != len(title_parts)

    should_run = bool(state.get("overwrite")) or not movie.get("imdb_url") or imdb_incomplete

    if should_run:
        result = imdb_links.search_one(
            movie_id,
            max_results=int(state.get("max_results", IMDB_MAX_RESULTS)),
            overwrite=True,
        )

        status = str(result.get("status", ""))
        if status not in {"found", "skipped"}:
            return _with_failure(
                movie_id,
                step="imdb",
                error=str(result.get("error") or "IMDb search failed"),
            )

    refreshed = movies.get_movie(movie_id)
    if _should_stop_after(state, "imdb"):
        return {
            "movie": refreshed,
            "stop_pipeline": True,
            "outcome": "stopped_after_imdb",
        }

    return {"movie": refreshed}



def _title_es_node(state: WorkflowState) -> WorkflowState:
    if state.get("failed_step") or state.get("stop_pipeline"):
        return {}

    if not _stage_enabled(state, "title_es"):
        return {}

    movie_id = state["movie_id"]
    movies.set_workflow_running(movie_id, node="fetch_imdb_title_es", action=state.get("action"))

    movie = movies.get_movie(movie_id)
    if movie is None:
        return _with_failure(movie_id, step="title_es", error="Movie disappeared during IMDb ES title fetch")

    imdb_url = str(movie.get("imdb_url") or "").strip()
    if not imdb_url:
        return _with_failure(movie_id, step="title_es", error="Missing imdb_url")

    title_es_parts = split_values(movie.get("imdb_title_es"))
    imdb_url_parts = split_values(imdb_url)
    title_es_incomplete = len(imdb_url_parts) > 1 and len(title_es_parts) != len(imdb_url_parts)

    should_run = (
        bool(state.get("overwrite"))
        or not str(movie.get("imdb_title_es") or "").strip()
        or title_es_incomplete
    )
    if should_run:
        # Non-blocking branch: if this fails, we keep the pipeline moving.
        imdb_title_es.fetch_one(movie_id, imdb_url=imdb_url)

    refreshed = movies.get_movie(movie_id)
    if _should_stop_after(state, "title_es"):
        return {
            "movie": refreshed,
            "stop_pipeline": True,
            "outcome": "stopped_after_title_es",
        }

    return {"movie": refreshed}


def _omdb_node(state: WorkflowState) -> WorkflowState:
    if state.get("failed_step") or state.get("stop_pipeline"):
        return {}

    if not _stage_enabled(state, "omdb"):
        return {}

    movie_id = state["movie_id"]
    movies.set_workflow_running(movie_id, node="fetch_omdb", action=state.get("action"))

    movie = movies.get_movie(movie_id)
    if movie is None:
        return _with_failure(movie_id, step="omdb", error="Movie disappeared during OMDb fetch")

    if not movie.get("imdb_id"):
        return _with_failure(movie_id, step="omdb", error="Missing imdb_id")

    imdb_id_parts = split_values(str(movie.get("imdb_id") or ""))
    omdb_title_parts = split_values(str(movie.get("omdb_title") or ""))
    omdb_incomplete = len(imdb_id_parts) > 1 and len(omdb_title_parts) != len(imdb_id_parts)

    should_run = bool(state.get("overwrite")) or movie.get("omdb_status") != "fetched" or omdb_incomplete

    if should_run:
        try:
            result = omdb_data.fetch_one(movie_id, imdb_id=movie.get("imdb_id"))
        except Exception as exc:
            return _with_failure(movie_id, step="omdb", error=str(exc))

        if result.get("status") != "fetched":
            return _with_failure(movie_id, step="omdb", error=str(result.get("error") or "OMDb fetch failed"))

    refreshed = movies.get_movie(movie_id)
    if _should_stop_after(state, "omdb"):
        return {
            "movie": refreshed,
            "stop_pipeline": True,
            "outcome": "stopped_after_omdb",
        }

    return {"movie": refreshed}



def _translation_node(state: WorkflowState) -> WorkflowState:
    if state.get("failed_step") or state.get("stop_pipeline"):
        return {}

    if not _stage_enabled(state, "translation"):
        return {}

    movie_id = state["movie_id"]
    movies.set_workflow_running(movie_id, node="translate_plot", action=state.get("action"))

    movie = movies.get_movie(movie_id)
    if movie is None:
        return _with_failure(movie_id, step="translation", error="Movie disappeared during translation")

    plot_en = (movie.get("omdb_plot_en") or "").strip()
    model = state.get("translation_model", TRANSLATION_MODEL)

    if not plot_en:
        movies.update_plot_translation(
            movie_id,
            plot_es=movie.get("omdb_plot_es"),
            status="skipped",
            error="No omdb_plot_en to translate",
        )
    else:
        plot_en_parts = split_values(plot_en, separator=PLOT_MULTI_SEPARATOR)
        plot_es_parts = split_values(str(movie.get("omdb_plot_es") or ""), separator=PLOT_MULTI_SEPARATOR)
        plot_es_incomplete = len(plot_en_parts) > 1 and len(plot_es_parts) != len(plot_en_parts)

        should_run = bool(state.get("overwrite")) or not movie.get("omdb_plot_es") or plot_es_incomplete
        if should_run:
            try:
                translated = plot_translation.translate_plot(plot_en, model=model)
            except Exception as exc:
                return _with_failure(movie_id, step="translation", error=str(exc))

            movies.update_plot_translation(
                movie_id,
                plot_es=translated,
                status="translated",
                error=None,
            )

    refreshed = movies.get_movie(movie_id)
    if _should_stop_after(state, "translation"):
        return {
            "movie": refreshed,
            "stop_pipeline": True,
            "outcome": "stopped_after_translation",
        }

    return {"movie": refreshed}



def _evaluate_node(state: WorkflowState) -> WorkflowState:
    movie_id = state["movie_id"]

    if state.get("outcome") == "approved":
        return {"route": "end"}

    failed_step = state.get("failed_step")
    error = state.get("error")

    if failed_step:
        if failed_step == "load_movie":
            return {"route": "end"}

        attempt = int(state.get("attempt") or 0)
        max_attempts_raw = state.get("max_attempts")
        max_attempts = WORKFLOW_MAX_ATTEMPTS if max_attempts_raw is None else int(max_attempts_raw)

        if failed_step != "apply_action" and attempt < max_attempts:
            return {"route": "retry"}

        review_reason = f"{failed_step}: {error or 'Unknown error'}"
        movies.set_workflow_review(
            movie_id,
            node=failed_step,
            reason=review_reason,
            error=error,
        )
        return {
            "route": "end",
            "outcome": "review",
        }

    if state.get("stop_pipeline"):
        stop_after = state.get("stop_after")
        if stop_after:
            movies.set_workflow_pending(
                movie_id,
                node=f"stage:{stop_after}",
                reason=f"Stopped after stage {stop_after}",
            )
        else:
            movies.set_workflow_pending(movie_id, node="paused")

        return {
            "route": "end",
            "outcome": "partial",
        }

    movies.set_workflow_done(movie_id, node="workflow_done")
    return {
        "route": "end",
        "outcome": "done",
    }



def _retry_node(state: WorkflowState) -> WorkflowState:
    movie_id = state["movie_id"]
    failed_step = state.get("failed_step") or "extraction"
    retry_stage = RETRY_STAGE_MAP.get(failed_step, "extraction")

    attempt = movies.increment_workflow_attempt(movie_id)
    movies.reset_from_stage(movie_id, retry_stage)
    movies.set_workflow_running(movie_id, node=f"retry_{retry_stage}", action="auto_retry")

    refreshed = movies.get_movie(movie_id)

    return {
        "movie": refreshed,
        "attempt": attempt,
        "start_stage": retry_stage,
        "failed_step": None,
        "error": None,
        "stop_pipeline": False,
        "outcome": "retry",
        "overwrite": True,
    }



def _route_after_evaluate(state: WorkflowState) -> Literal["retry", "end"]:
    return state.get("route", "end")



def _build_graph():
    builder = StateGraph(WorkflowState)

    builder.add_node("load_movie", _load_movie_node)
    builder.add_node("apply_action", _apply_action_node)
    builder.add_node("extract", _extract_node)
    builder.add_node("imdb", _imdb_node)
    builder.add_node("title_es", _title_es_node)
    builder.add_node("omdb", _omdb_node)
    builder.add_node("translation", _translation_node)
    builder.add_node("evaluate", _evaluate_node)
    builder.add_node("retry", _retry_node)

    builder.set_entry_point("load_movie")
    builder.add_edge("load_movie", "apply_action")
    builder.add_edge("apply_action", "extract")
    builder.add_edge("extract", "imdb")
    builder.add_edge("imdb", "title_es")
    builder.add_edge("title_es", "omdb")
    builder.add_edge("omdb", "translation")
    builder.add_edge("translation", "evaluate")

    builder.add_conditional_edges(
        "evaluate",
        _route_after_evaluate,
        {
            "retry": "retry",
            "end": END,
        },
    )

    builder.add_edge("retry", "extract")

    return builder.compile()



def get_workflow_graph():
    global _GRAPH
    if _GRAPH is None:
        _GRAPH = _build_graph()
    return _GRAPH



def run_workflow_graph(initial_state: WorkflowState) -> WorkflowState:
    graph = get_workflow_graph()
    return graph.invoke(initial_state)
