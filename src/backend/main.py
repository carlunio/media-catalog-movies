from fastapi import FastAPI, HTTPException

from src.project_meta import get_app_meta

from .clients import ClientError, list_ollama_models
from .config import (
    TRANSLATION_MODEL,
    VISION_TEAM_MODEL,
    VISION_TITLE_MODEL,
    WORKFLOW_MAX_ATTEMPTS,
)
from .schemas.imdb import ManualImdbRequest, ManualImdbTitleEsRequest, RunImdbRequest
from .schemas.ingest import IngestRequest, RebindCoversRequest, RunExtractRequest
from .schemas.omdb import RunOmdbRequest, UpdateOmdbRequest
from .schemas.review import UpdateTitleTeamRequest
from .schemas.translation import RunTranslationRequest, UpdatePlotTranslationRequest
from .schemas.workflow import (
    WorkflowMarkReviewRequest,
    WorkflowReviewRequest,
    WorkflowRunRequest,
)
from .routers import export as export_router
from .routers import items as items_router
from .routers import snapshots as snapshots_router
from .services import catalog, migrations, movies, workflow

APP_META = get_app_meta()

app = FastAPI(title=f"{APP_META.app_name} API", version=APP_META.version)

migrations.migrate()
movies.init_table()
catalog.init_table()
_recovered_stale_runs = movies.recover_stale_running_workflows()
if _recovered_stale_runs:
    print(f"[startup] recovered {_recovered_stale_runs} stale workflow runs")

app.include_router(items_router.router)
app.include_router(export_router.router)
app.include_router(snapshots_router.router)


def _resolve_max_attempts(value: int | None) -> int:
    return WORKFLOW_MAX_ATTEMPTS if value is None else int(value)


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


@app.post("/covers/read")
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


@app.post("/covers/rebind")
def rebind_covers(payload: RebindCoversRequest):
    try:
        return movies.rebind_image_paths(
            covers_dir=payload.folder,
            recursive=payload.recursive,
            extensions=payload.extensions,
            limit=payload.limit,
            only_missing=payload.only_missing,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/covers/name-audit")
def covers_name_audit():
    try:
        return movies.audit_cover_name_format()
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
            max_attempts=_resolve_max_attempts(payload.max_attempts),
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
        raise HTTPException(status_code=400, detail="limit debe estar entre 1 y 50000")
    if review_limit < 1 or review_limit > 5000:
        raise HTTPException(
            status_code=400, detail="review_limit debe estar entre 1 y 5000"
        )
    return workflow.snapshot(limit=limit, review_limit=review_limit)


@app.post("/workflow/review/{movie_id}")
def workflow_review_action(movie_id: str, payload: WorkflowReviewRequest):
    if movies.get_movie(movie_id) is None:
        raise HTTPException(status_code=404, detail="Película no encontrada")

    try:
        result = workflow.review_action(
            movie_id,
            action=payload.action,
            max_attempts=_resolve_max_attempts(payload.max_attempts),
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
        raise HTTPException(status_code=404, detail="Película no encontrada")

    try:
        result = workflow.mark_review(
            movie_id, reason=payload.reason, node=payload.node
        )
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
        raise HTTPException(status_code=404, detail="Película no encontrada")
    return movie


@app.put("/movies/{movie_id}/title-team")
def update_title_team(movie_id: str, payload: UpdateTitleTeamRequest):
    if movies.get_movie(movie_id) is None:
        raise HTTPException(status_code=404, detail="Película no encontrada")

    movies.update_title_team(movie_id, payload.title, payload.team)
    return {"ok": True}


@app.put("/movies/{movie_id}/imdb")
def set_manual_imdb(movie_id: str, payload: ManualImdbRequest):
    if movies.get_movie(movie_id) is None:
        raise HTTPException(status_code=404, detail="Película no encontrada")

    try:
        movies.set_manual_imdb(movie_id, payload.imdb_url)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return {"ok": True}


@app.put("/movies/{movie_id}/imdb-title-es")
def set_manual_imdb_title_es(movie_id: str, payload: ManualImdbTitleEsRequest):
    if movies.get_movie(movie_id) is None:
        raise HTTPException(status_code=404, detail="Película no encontrada")

    movies.set_manual_imdb_title_es(movie_id, payload.title_es)
    return {"ok": True}


@app.put("/movies/{movie_id}/omdb")
def update_omdb(movie_id: str, payload: UpdateOmdbRequest):
    if movies.get_movie(movie_id) is None:
        raise HTTPException(status_code=404, detail="Película no encontrada")

    movies.update_omdb_fields(movie_id, payload.fields)
    return {"ok": True}


@app.put("/movies/{movie_id}/plot-es")
def update_plot_es(movie_id: str, payload: UpdatePlotTranslationRequest):
    if movies.get_movie(movie_id) is None:
        raise HTTPException(status_code=404, detail="Película no encontrada")

    movies.update_plot_translation(
        movie_id,
        plot_es=payload.plot_es,
        status="manual",
        error=None,
    )
    return {"ok": True}
