from pathlib import Path

from fastapi import FastAPI, HTTPException

from .config import TRANSLATION_MODEL, VISION_TEAM_MODEL, VISION_TITLE_MODEL
from .schemas.imdb import ManualImdbRequest, RunImdbRequest
from .schemas.ingest import IngestRequest, RunExtractRequest
from .schemas.omdb import RunOmdbRequest, UpdateOmdbRequest
from .schemas.review import UpdateTitleTeamRequest
from .schemas.translation import RunTranslationRequest, UpdatePlotTranslationRequest
from .services import cover_extraction, export, imdb_links, movies, omdb_data, plot_translation

app = FastAPI(title="Media Catalog Movies API", version="0.1.0")

movies.init_table()


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/stats")
def stats() -> dict[str, int]:
    return movies.get_stats()


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


@app.post("/extract/run")
def run_extraction(payload: RunExtractRequest):
    try:
        return cover_extraction.run_batch(
            movie_id=payload.movie_id,
            limit=payload.limit,
            overwrite=payload.overwrite,
            title_model=payload.title_model or VISION_TITLE_MODEL,
            team_model=payload.team_model or VISION_TEAM_MODEL,
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


@app.post("/imdb/search")
def run_imdb_search(payload: RunImdbRequest):
    try:
        return imdb_links.run_batch(
            movie_id=payload.movie_id,
            limit=payload.limit,
            overwrite=payload.overwrite,
            max_results=payload.max_results,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.put("/movies/{movie_id}/imdb")
def set_manual_imdb(movie_id: str, payload: ManualImdbRequest):
    if movies.get_movie(movie_id) is None:
        raise HTTPException(status_code=404, detail="Movie not found")

    try:
        movies.set_manual_imdb(movie_id, payload.imdb_url)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return {"ok": True}


@app.post("/omdb/fetch")
def fetch_omdb(payload: RunOmdbRequest):
    try:
        return omdb_data.run_batch(
            movie_id=payload.movie_id,
            limit=payload.limit,
            overwrite=payload.overwrite,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.put("/movies/{movie_id}/omdb")
def update_omdb(movie_id: str, payload: UpdateOmdbRequest):
    if movies.get_movie(movie_id) is None:
        raise HTTPException(status_code=404, detail="Movie not found")

    movies.update_omdb_fields(movie_id, payload.fields)
    return {"ok": True}


@app.post("/plot/translate")
def translate_plot(payload: RunTranslationRequest):
    try:
        return plot_translation.run_batch(
            movie_id=payload.movie_id,
            limit=payload.limit,
            overwrite=payload.overwrite,
            model=payload.model or TRANSLATION_MODEL,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


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
