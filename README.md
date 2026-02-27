# media-catalog-movies

Movie catalog pipeline inspired by `media-catalog-vinyls`.

## Pipeline

1. Ingest cover files into DuckDB.
2. Run LangGraph workflow nodes (`extraction -> imdb -> omdb -> translation`).
3. Mark records that require manual review and retry from chosen stage.
4. Review and correct title/team with cover preview.
5. Review or override IMDb/OMDb fields.
6. Export catalog.

## Quick start

```bash
cp .env.example .env
make setup
make dev-back
# in another terminal
make dev-front
```

Example source folder for your legacy data:

- `../peliculas/movie_catalog_v0.1/data/input`

## Data model

All stages are persisted in `data/movies.duckdb` (table `movies`).
Workflow state and retries are stored in `workflow_*` columns in the same table.

## Workflow API

- `POST /workflow/run`: run LangGraph pipeline for one movie or a batch.
- `GET /workflow/graph`: graph metadata used by Streamlit orchestration page.
- `GET /workflow/snapshot`: aggregated counts by stage/status + review queue.
- `POST /workflow/review/{movie_id}`: approve or retry from a chosen stage.
- `POST /workflow/review/{movie_id}/mark`: manually mark one movie as review.
- Legacy endpoints (`/extract/run`, `/imdb/search`, `/omdb/fetch`, `/plot/translate`) are kept and internally mapped to LangGraph stage runs.

## Streamlit flow

- `Fase 0 - Orquestacion LangGraph`: graph view, workflow run controls, stage board, and review queue actions.
- `Fase 1 - Ingesta`: ingest cover files from folder into DuckDB.
- `Fase 2+`: manual revision and stage-specific pages.
- Sidebar `HTTP timeout`: choose `Normal`, `Unitario` (same timeout for all requests), or `Desactivado` (no timeout).

## Environment variables

- `DB_PATH`: DuckDB file path.
- `COVERS_DIR`: default folder for cover ingestion.
- `OMDB_API_KEY`: required to fetch OMDb.
- `OMDB_PLOT_MODE`: `full` (recommended) or `short` for OMDb plot length.
- `VISION_TITLE_MODEL`: Ollama model used for title extraction.
- `VISION_TEAM_MODEL`: Ollama model used for team extraction.
- `TRANSLATION_MODEL`: Ollama model used for plot translation.
- `IMDB_MAX_RESULTS`: max results checked per IMDb search attempt.
- `IMDB_SLEEP_SECONDS`: delay between items during batch IMDb search.
- `REQUEST_TIMEOUT_SECONDS`: backend HTTP timeout (used for IMDb/OMDb calls).
- `WORKFLOW_MAX_ATTEMPTS`: max automatic retries before moving to review state.
- `API_URL`: frontend target backend URL.
- `API_TIMEOUT_SECONDS`: default timeout for frontend -> backend requests.
- `API_LONG_TIMEOUT_SECONDS`: timeout used by heavy jobs like cover extraction.

Records that require manual intervention are marked with:
- `workflow_status = review`
- `workflow_needs_review = true`
- `workflow_review_reason` with the failing node/cause
- `pipeline_stage` derived field (`extraction`, `imdb`, `omdb`, `translation`, `review`, `done`)
