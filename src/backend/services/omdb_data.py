from typing import Any

from ..clients import http_get_json
from ..config import OMDB_API_KEY, OMDB_PLOT_MODE
from ..multi_value import PLOT_MULTI_SEPARATOR, join_values, split_values
from . import movies

OMDB_FIELDS = [
    "Title",
    "Year",
    "Rated",
    "Released",
    "Runtime",
    "Genre",
    "Director",
    "Writer",
    "Actors",
    "Plot",
    "Language",
    "Country",
    "Awards",
    "Poster",
    "imdbRating",
    "imdbVotes",
    "Type",
    "DVD",
    "BoxOffice",
    "Production",
]


def _fetch_payload(imdb_id: str) -> dict[str, Any]:
    payload = http_get_json(
        "https://www.omdbapi.com/",
        params={"i": imdb_id, "apikey": OMDB_API_KEY, "plot": OMDB_PLOT_MODE},
    )
    return payload


def _aggregate_payloads(payloads: list[dict[str, Any]]) -> dict[str, Any]:
    if not payloads:
        return {}

    if len(payloads) == 1:
        return payloads[0]

    aggregated: dict[str, Any] = {
        "Response": "True",
        "_items": payloads,
    }

    for key in OMDB_FIELDS:
        values = [str(item.get(key) or "").strip() for item in payloads]
        separator = PLOT_MULTI_SEPARATOR if key == "Plot" else ";"
        aggregated[key] = join_values(values, separator=separator, keep_empty=True)

    return aggregated


def fetch_one(movie_id: str, imdb_id: str | None = None) -> dict[str, Any]:
    movie = movies.get_movie(movie_id)
    if not movie:
        raise ValueError(f"Movie not found: {movie_id}")

    target_imdb_ids = split_values(imdb_id or movie.get("imdb_id"))
    if not target_imdb_ids:
        raise ValueError(f"Movie {movie_id} has no imdb_id")

    if not OMDB_API_KEY:
        raise RuntimeError("OMDB_API_KEY is missing")

    payloads: list[dict[str, Any]] = []
    errors: list[str] = []

    for imdb_item in target_imdb_ids:
        payload = _fetch_payload(imdb_item)
        if payload.get("Response") != "True":
            error = payload.get("Error", "Unknown OMDb error")
            errors.append(f"{imdb_item}: {error}")
            continue
        payloads.append(payload)

    if errors:
        movies.update_omdb(movie_id, {}, status="error", error=" | ".join(errors))
        return {
            "id": movie_id,
            "status": "error",
            "imdb_id": imdb_id or movie.get("imdb_id"),
            "error": " | ".join(errors),
        }

    combined_payload = _aggregate_payloads(payloads)
    movies.update_omdb(movie_id, combined_payload, status="fetched", error=None)

    return {
        "id": movie_id,
        "status": "fetched",
        "imdb_id": imdb_id or movie.get("imdb_id"),
        "title": combined_payload.get("Title"),
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
