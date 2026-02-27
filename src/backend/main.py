from pathlib import Path

from fastapi import FastAPI, HTTPException

from .clients import ClientError, list_ollama_models
from .config import (
    TRANSLATION_MODEL,
    VISION_TEAM_MODEL,
    VISION_TITLE_MODEL,
    WORKFLOW_MAX_ATTEMPTS,
)
from .schemas.imdb import ManualImdbRequest, RunImdbRequest
from .schemas.ingest import IngestRequest, RunExtractRequest
from .schemas.omdb import RunOmdbRequest, UpdateOmdbRequest
from .schemas.review import UpdateTitleTeamRequest
from .schemas.translation import RunTranslationRequest, UpdatePlotTranslationRequest
from .schemas.workflow import WorkflowMarkReviewRequest, WorkflowReviewRequest, WorkflowRunRequest
from .services import export, movies, workflow

app = FastAPI(title="Media Catalog Movies API", version="0.2.0")

movies.init_table()


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/stats")
def stats() -> dict[str, int]:
    return movies.get_stats()


@app.get("/models/ollama")
def ollama_models():
    try:
        return {"models": list_ollama_models()}
    except ClientError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/covers/ingest")
def ingest_covers(payload: IngestRequest):
    try:
        return movies.ingest_covers(
            payload.folder,
            recursive=payload.recursive,
            extensions=payload.extensions,
            overwrite_existing_paths=payload.overwrite_existing_paths,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/workflow/run")
def workflow_run(payload: WorkflowRunRequest):
    try:
        return workflow.run_batch(
            movie_id=payload.movie_id,
            limit=payload.limit,
            start_stage=payload.start_stage,
            stop_after=payload.stop_after,
            action=payload.action,
            overwrite=payload.overwrite,
            title_model=payload.title_model or VISION_TITLE_MODEL,
            team_model=payload.team_model or VISION_TEAM_MODEL,
            translation_model=payload.translation_model or TRANSLATION_MODEL,
            max_results=payload.max_results,
            max_attempts=payload.max_attempts or WORKFLOW_MAX_ATTEMPTS,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/workflow/graph")
def workflow_graph():
    return workflow.graph_definition()


@app.get("/workflow/snapshot")
def workflow_snapshot(limit: int = 5000, review_limit: int = 200):
    if limit < 1 or limit > 50000:
        raise HTTPException(status_code=400, detail="limit must be between 1 and 50000")
    if review_limit < 1 or review_limit > 5000:
        raise HTTPException(status_code=400, detail="review_limit must be between 1 and 5000")
    return workflow.snapshot(limit=limit, review_limit=review_limit)


@app.post("/workflow/review/{movie_id}")
def workflow_review_action(movie_id: str, payload: WorkflowReviewRequest):
    if movies.get_movie(movie_id) is None:
        raise HTTPException(status_code=404, detail="Movie not found")

    try:
        result = workflow.review_action(
            movie_id,
            action=payload.action,
            max_attempts=payload.max_attempts or WORKFLOW_MAX_ATTEMPTS,
            title_model=payload.title_model or VISION_TITLE_MODEL,
            team_model=payload.team_model or VISION_TEAM_MODEL,
            translation_model=payload.translation_model or TRANSLATION_MODEL,
            max_results=payload.max_results,
        )
        return {"ok": True, "result": result}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/workflow/review/{movie_id}/mark")
def workflow_mark_review(movie_id: str, payload: WorkflowMarkReviewRequest):
    if movies.get_movie(movie_id) is None:
        raise HTTPException(status_code=404, detail="Movie not found")

    try:
        result = workflow.mark_review(movie_id, reason=payload.reason, node=payload.node)
        return {"ok": True, "result": result}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# -------------------------
# Legacy-compatible endpoints
# -------------------------
@app.post("/extract/run")
def run_extraction(payload: RunExtractRequest):
    try:
        return workflow.run_batch(
            movie_id=payload.movie_id,
            limit=payload.limit,
            start_stage="extraction",
            stop_after="extraction",
            overwrite=payload.overwrite,
            title_model=payload.title_model or VISION_TITLE_MODEL,
            team_model=payload.team_model or VISION_TEAM_MODEL,
            max_attempts=WORKFLOW_MAX_ATTEMPTS,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/imdb/search")
def run_imdb_search(payload: RunImdbRequest):
    try:
        return workflow.run_batch(
            movie_id=payload.movie_id,
            limit=payload.limit,
            start_stage="imdb",
            stop_after="imdb",
            overwrite=payload.overwrite,
            max_results=payload.max_results,
            max_attempts=WORKFLOW_MAX_ATTEMPTS,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/omdb/fetch")
def fetch_omdb(payload: RunOmdbRequest):
    try:
        return workflow.run_batch(
            movie_id=payload.movie_id,
            limit=payload.limit,
            start_stage="omdb",
            stop_after="omdb",
            overwrite=payload.overwrite,
            max_attempts=WORKFLOW_MAX_ATTEMPTS,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/plot/translate")
def translate_plot(payload: RunTranslationRequest):
    try:
        return workflow.run_batch(
            movie_id=payload.movie_id,
            limit=payload.limit,
            start_stage="translation",
            stop_after="translation",
            overwrite=payload.overwrite,
            translation_model=payload.model or TRANSLATION_MODEL,
            max_attempts=WORKFLOW_MAX_ATTEMPTS,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/movies")
def list_movies(stage: str | None = None, limit: int = 500):
    return movies.list_movies(stage=stage, limit=limit)


@app.get("/movies/{movie_id}")
def get_movie(movie_id: str):
    movie = movies.get_movie(movie_id)
    if movie is None:
        raise HTTPException(status_code=404, detail="Movie not found")
    return movie


@app.put("/movies/{movie_id}/title-team")
def update_title_team(movie_id: str, payload: UpdateTitleTeamRequest):
    if movies.get_movie(movie_id) is None:
        raise HTTPException(status_code=404, detail="Movie not found")

    movies.update_title_team(movie_id, payload.title, payload.team)
    return {"ok": True}


@app.put("/movies/{movie_id}/imdb")
def set_manual_imdb(movie_id: str, payload: ManualImdbRequest):
    if movies.get_movie(movie_id) is None:
        raise HTTPException(status_code=404, detail="Movie not found")

    try:
        movies.set_manual_imdb(movie_id, payload.imdb_url)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return {"ok": True}


@app.put("/movies/{movie_id}/omdb")
def update_omdb(movie_id: str, payload: UpdateOmdbRequest):
    if movies.get_movie(movie_id) is None:
        raise HTTPException(status_code=404, detail="Movie not found")

    movies.update_omdb_fields(movie_id, payload.fields)
    return {"ok": True}


@app.put("/movies/{movie_id}/plot-es")
def update_plot_es(movie_id: str, payload: UpdatePlotTranslationRequest):
    if movies.get_movie(movie_id) is None:
        raise HTTPException(status_code=404, detail="Movie not found")

    movies.update_plot_translation(
        movie_id,
        plot_es=payload.plot_es,
        model="manual",
        status="manual",
        error=None,
    )
    return {"ok": True}


@app.get("/export/movies/tsv")
def export_tsv():
    output = Path("exports/movies.tsv")
    path = export.export_movies_tsv(output)
    return {"ok": True, "path": str(path)}
