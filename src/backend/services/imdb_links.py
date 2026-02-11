import re
import time
import unicodedata
from typing import Any
from urllib.parse import parse_qs, unquote, urlsplit

import requests

from ..config import IMDB_MAX_RESULTS, IMDB_SLEEP_SECONDS, REQUEST_TIMEOUT_SECONDS
from ..normalizers import canonical_imdb_url, extract_imdb_id
from . import movies

try:
    from googlesearch import search as google_search
except Exception:  # pragma: no cover
    google_search = None

IMDB_SITE_FILTER = "site:imdb.com/title"
IMDB_FIND_URL = "https://www.imdb.com/find/"
IMDB_ID_FROM_HTML = re.compile(r"/title/(tt\d{7,8})\b", re.IGNORECASE)
IMDB_REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}


def _dedupe_keep_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        if value not in seen:
            ordered.append(value)
            seen.add(value)
    return ordered


def _normalize_for_search(text: str) -> str:
    decomposed = unicodedata.normalize("NFKD", text)
    stripped = "".join(ch for ch in decomposed if not unicodedata.combining(ch))
    return " ".join(stripped.split())


def _canonical_from_candidate(value: str) -> str | None:
    candidates: list[str] = [value]
    parsed = urlsplit(value)
    for key in ("q", "url", "u"):
        candidates.extend(parse_qs(parsed.query).get(key, []))

    for raw in candidates:
        decoded = unquote(raw).strip()
        if not decoded:
            continue
        canonical = canonical_imdb_url(decoded)
        if canonical:
            return canonical

        imdb_id = extract_imdb_id(decoded)
        if imdb_id:
            return f"https://www.imdb.com/title/{imdb_id}/"

    return None


def _build_search_terms(title: str | None, team: list[str]) -> list[str]:
    clean_title = " ".join((title or "").strip().split())
    clean_team = [" ".join(member.strip().split()) for member in team if member and member.strip()]
    lead_team = " ".join(clean_team[:2]).strip()

    terms: list[str] = []
    has_title = bool(clean_title) and not clean_title.upper().startswith("NO IDENTIFICADO")
    if has_title:
        terms.append(" ".join(part for part in [clean_title, lead_team] if part))
        terms.append(clean_title)

        ascii_title = _normalize_for_search(clean_title)
        if ascii_title and ascii_title.lower() != clean_title.lower():
            terms.append(" ".join(part for part in [ascii_title, lead_team] if part))
            terms.append(ascii_title)

    if clean_team:
        terms.append(" ".join(clean_team[:3]))

    return _dedupe_keep_order([term for term in terms if term])


def _build_google_queries(search_terms: list[str]) -> list[str]:
    return [f"{term} {IMDB_SITE_FILTER}".strip() for term in search_terms if term]


def _find_best_imdb_url_google(
    google_queries: list[str],
    max_results: int,
) -> tuple[str | None, str | None, bool]:
    if google_search is None:
        return None, None, False

    saw_results = False
    for query in google_queries:
        links = list(google_search(query, num_results=max_results))
        if links:
            saw_results = True

        for link in links:
            canonical = _canonical_from_candidate(str(link))
            if canonical:
                return canonical, query, saw_results

    return None, None, saw_results


def _extract_imdb_urls_from_html(html: str, max_results: int) -> list[str]:
    urls: list[str] = []
    seen_ids: set[str] = set()

    for match in IMDB_ID_FROM_HTML.findall(html):
        imdb_id = match.lower()
        if imdb_id in seen_ids:
            continue
        seen_ids.add(imdb_id)
        urls.append(f"https://www.imdb.com/title/{imdb_id}/")
        if len(urls) >= max_results:
            break

    return urls


def _find_best_imdb_url_imdb_find(
    search_terms: list[str],
    max_results: int,
) -> tuple[str | None, str | None, bool]:
    saw_candidates = False
    for term in search_terms:
        response = requests.get(
            IMDB_FIND_URL,
            params={"q": term, "s": "tt", "ttype": "ft", "ref_": "fn_ft"},
            headers=IMDB_REQUEST_HEADERS,
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()

        candidates = _extract_imdb_urls_from_html(response.text, max_results=max_results)
        if candidates:
            saw_candidates = True
            return candidates[0], term, saw_candidates

    return None, None, saw_candidates


def _search_and_store(movie_row: dict[str, Any], max_results: int) -> dict[str, Any]:
    movie_id = movie_row["id"]
    title = movie_row.get("manual_title") or movie_row.get("extraction_title")
    team = movie_row.get("manual_team") or movie_row.get("extraction_team") or []

    search_terms = _build_search_terms(title, team)
    google_queries = _build_google_queries(search_terms)

    if not search_terms:
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

    google_error: str | None = None
    imdb_find_error: str | None = None
    google_saw_results = False
    imdb_find_saw_candidates = False

    try:
        best_url, used_google_query, google_saw_results = _find_best_imdb_url_google(
            google_queries,
            max_results=max_results,
        )
    except Exception as exc:
        best_url = None
        used_google_query = None
        google_error = str(exc)

    if best_url:
        movies.update_imdb(
            movie_id,
            imdb_query=used_google_query or google_queries[0],
            imdb_url=best_url,
            imdb_status="found",
            imdb_last_error=None,
        )
        return {
            "id": movie_id,
            "status": "found",
            "query": used_google_query or google_queries[0],
            "imdb_url": best_url,
            "source": "google",
        }

    try:
        best_url, used_imdb_find_term, imdb_find_saw_candidates = _find_best_imdb_url_imdb_find(
            search_terms,
            max_results=max_results,
        )
    except Exception as exc:
        best_url = None
        used_imdb_find_term = None
        imdb_find_error = str(exc)

    if best_url:
        movies.update_imdb(
            movie_id,
            imdb_query=f"imdb-find:{used_imdb_find_term or search_terms[0]}",
            imdb_url=best_url,
            imdb_status="found",
            imdb_last_error=None,
        )
        return {
            "id": movie_id,
            "status": "found",
            "query": f"imdb-find:{used_imdb_find_term or search_terms[0]}",
            "imdb_url": best_url,
            "source": "imdb-find",
        }

    first_query = google_queries[0] if google_queries else search_terms[0]

    errors = [message for message in [google_error, imdb_find_error] if message]
    if errors and not google_saw_results and not imdb_find_saw_candidates:
        error_text = " ; ".join(errors)
        movies.update_imdb(
            movie_id,
            imdb_query=first_query,
            imdb_url=None,
            imdb_status="error",
            imdb_last_error=error_text,
        )
        return {
            "id": movie_id,
            "status": "error",
            "query": first_query,
            "error": error_text,
        }

    movies.update_imdb(
        movie_id,
        imdb_query=first_query,
        imdb_url=None,
        imdb_status="not_found",
        imdb_last_error="No IMDb URL found after trying Google and IMDb find",
    )
    return {
        "id": movie_id,
        "status": "not_found",
        "query": first_query,
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
