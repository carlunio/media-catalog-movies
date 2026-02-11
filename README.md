# media-catalog-movies

Movie catalog pipeline inspired by `media-catalog-vinyls`.

## Pipeline

1. Ingest cover files into DuckDB.
2. Extract title and team from cover images (Ollama vision).
3. Review and correct title/team with cover preview.
4. Search and validate IMDb link.
5. Fetch OMDb data by IMDb ID.
6. Review OMDb data.
7. Translate plot to Spanish.
8. Export catalog.

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

## Environment variables

- `DB_PATH`: DuckDB file path.
- `COVERS_DIR`: default folder for cover ingestion.
- `OMDB_API_KEY`: required to fetch OMDb.
- `VISION_TITLE_MODEL`: Ollama model used for title extraction.
- `VISION_TEAM_MODEL`: Ollama model used for team extraction.
- `TRANSLATION_MODEL`: Ollama model used for plot translation.
- `IMDB_MAX_RESULTS`: max results checked per IMDb search attempt.
- `IMDB_SLEEP_SECONDS`: delay between items during batch IMDb search.
- `REQUEST_TIMEOUT_SECONDS`: backend HTTP timeout (used for IMDb/OMDb calls).
- `API_URL`: frontend target backend URL.
- `API_TIMEOUT_SECONDS`: default timeout for frontend -> backend requests.
- `API_LONG_TIMEOUT_SECONDS`: timeout used by heavy jobs like cover extraction.
