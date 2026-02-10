import time
from typing import Any

from ..config import IMDB_MAX_RESULTS, IMDB_SLEEP_SECONDS
from ..normalizers import canonical_imdb_url
from . import movies

try:
    from googlesearch import search as google_search
except Exception:  # pragma: no cover
    google_search = None


def _build_query(title: str | None, team: list[str]) -> str:
    clean_title = (title or "").strip()
    clean_team = [member.strip() for member in team if member and member.strip()]

    if not clean_title or clean_title.upper().startswith("NO IDENTIFICADO"):
        return f"{' '.join(clean_team[:3])} site:imdb.com/title".strip()

    return f"{clean_title} {' '.join(clean_team[:1])} site:imdb.com/title".strip()


def _find_best_imdb_url(query: str, max_results: int) -> str | None:
    if google_search is None:
        raise RuntimeError("googlesearch-python is not installed")

    links = list(google_search(query, num_results=max_results))
    for link in links:
        canonical = canonical_imdb_url(link)
        if canonical:
            return canonical
    return None


def _search_and_store(movie_row: dict[str, Any], max_results: int) -> dict[str, Any]:
    movie_id = movie_row["id"]
    title = movie_row.get("manual_title") or movie_row.get("extraction_title")
    team = movie_row.get("manual_team") or movie_row.get("extraction_team") or []

    query = _build_query(title, team)
    if not query:
        movies.update_imdb(
            movie_id,
            imdb_query="",
            imdb_url=None,
            imdb_status="error",
            imdb_last_error="Not enough metadata to build query",
        )
        return {
            "id": movie_id,
            "status": "error",
            "query": "",
            "error": "Not enough metadata",
        }

    try:
        best_url = _find_best_imdb_url(query, max_results=max_results)
    except Exception as exc:
        movies.update_imdb(
            movie_id,
            imdb_query=query,
            imdb_url=None,
            imdb_status="error",
            imdb_last_error=str(exc),
        )
        return {
            "id": movie_id,
            "status": "error",
            "query": query,
            "error": str(exc),
        }

    if best_url:
        movies.update_imdb(
            movie_id,
            imdb_query=query,
            imdb_url=best_url,
            imdb_status="found",
            imdb_last_error=None,
        )
        return {
            "id": movie_id,
            "status": "found",
            "query": query,
            "imdb_url": best_url,
        }

    movies.update_imdb(
        movie_id,
        imdb_query=query,
        imdb_url=None,
        imdb_status="not_found",
        imdb_last_error="No IMDb URL found in top results",
    )
    return {
        "id": movie_id,
        "status": "not_found",
        "query": query,
    }


def run_batch(
    *,
    limit: int,
    overwrite: bool = False,
    movie_id: str | None = None,
    max_results: int = IMDB_MAX_RESULTS,
    sleep_seconds: float = IMDB_SLEEP_SECONDS,
) -> dict[str, Any]:
    if movie_id:
        one = movies.get_movie(movie_id)
        targets = [] if one is None else [one]
    else:
        targets = movies.movies_for_imdb(limit=limit, overwrite=overwrite)

    results: list[dict[str, Any]] = []
    for row in targets:
        result = _search_and_store(row, max_results=max_results)
        results.append(result)
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)

    return {
        "requested": len(targets),
        "processed": len(results),
        "items": results,
    }
