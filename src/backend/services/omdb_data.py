from typing import Any

from ..clients import http_get_json
from ..config import OMDB_API_KEY, OMDB_PLOT_MODE
from . import movies


def fetch_one(movie_id: str, imdb_id: str | None = None) -> dict[str, Any]:
    movie = movies.get_movie(movie_id)
    if not movie:
        raise ValueError(f"Movie not found: {movie_id}")

    target_imdb_id = imdb_id or movie.get("imdb_id")
    if not target_imdb_id:
        raise ValueError(f"Movie {movie_id} has no imdb_id")

    if not OMDB_API_KEY:
        raise RuntimeError("OMDB_API_KEY is missing")

    payload = http_get_json(
        "https://www.omdbapi.com/",
        params={"i": target_imdb_id, "apikey": OMDB_API_KEY, "plot": OMDB_PLOT_MODE},
    )

    if payload.get("Response") != "True":
        error = payload.get("Error", "Unknown OMDb error")
        movies.update_omdb(movie_id, payload, status="error", error=str(error))
        return {
            "id": movie_id,
            "status": "error",
            "imdb_id": target_imdb_id,
            "error": str(error),
        }

    movies.update_omdb(movie_id, payload, status="fetched", error=None)
    return {
        "id": movie_id,
        "status": "fetched",
        "imdb_id": target_imdb_id,
        "title": payload.get("Title"),
    }


def run_batch(
    *,
    limit: int,
    overwrite: bool = False,
    movie_id: str | None = None,
) -> dict[str, Any]:
    if movie_id:
        movie = movies.get_movie(movie_id)
        targets = [] if movie is None else [{"id": movie_id, "imdb_id": movie.get("imdb_id")}]
    else:
        targets = movies.movies_for_omdb(limit=limit, overwrite=overwrite)

    output: list[dict[str, Any]] = []
    for row in targets:
        mid = row["id"]
        imdb_id = row.get("imdb_id")
        try:
            output.append(fetch_one(mid, imdb_id=imdb_id))
        except Exception as exc:
            movies.update_omdb(mid, {}, status="error", error=str(exc))
            output.append({"id": mid, "status": "error", "error": str(exc)})

    return {
        "requested": len(targets),
        "processed": len(output),
        "items": output,
    }
