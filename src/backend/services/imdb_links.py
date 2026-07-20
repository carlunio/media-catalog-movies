import html as html_lib
import re
import time
import unicodedata
from typing import Any
from urllib.parse import parse_qs, unquote, urlsplit

import requests
from bs4 import BeautifulSoup

from ..config import IMDB_MAX_RESULTS, IMDB_SLEEP_SECONDS, REQUEST_TIMEOUT_SECONDS
from ..multi_value import join_values, split_values
from ..normalizers import canonical_imdb_url, extract_imdb_id
from . import movies

try:
    from googlesearch import search as google_search
except Exception:  # pragma: no cover
    google_search = None

try:
    from imdb import IMDb as CinemagoerIMDb
except Exception:  # pragma: no cover
    CinemagoerIMDb = None

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


def _parse_team_segment(value: str) -> list[str]:
    normalized = value.replace("\n", ",")
    return [part.strip() for part in normalized.split(",") if part.strip()]


def _flatten_team_values(team_values: list[str]) -> list[str]:
    out: list[str] = []
    for value in team_values:
        out.extend(_parse_team_segment(value))
    return out


def _team_segments_for_titles(team_values: list[str], title_count: int) -> list[list[str]]:
    if title_count <= 1:
        return [_flatten_team_values(team_values)]

    if len(team_values) == title_count:
        return [_parse_team_segment(segment) for segment in team_values]

    merged = join_values(team_values)
    merged_parts = split_values(merged)
    if len(merged_parts) == title_count:
        return [_parse_team_segment(segment) for segment in merged_parts]

    fallback = _flatten_team_values(team_values)
    return [fallback for _ in range(title_count)]


def _split_titles(title: str | None) -> list[str]:
    titles = split_values(title)
    if titles:
        return titles

    fallback = " ".join(str(title or "").split())
    return [fallback] if fallback else []


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
    query_errors: list[str] = []
    for query in google_queries:
        try:
            links = list(google_search(query, num_results=max_results))
        except Exception as exc:
            query_errors.append(f"{query}: {exc}")
            continue

        if links:
            saw_results = True

        for link in links:
            canonical = _canonical_from_candidate(str(link))
            if canonical:
                return canonical, query, saw_results

    if query_errors and len(query_errors) == len(google_queries) and not saw_results:
        raise RuntimeError("Google search failed for every query: " + " | ".join(query_errors))

    return None, None, saw_results


def _imdb_id_url(imdb_id: str) -> str | None:
    text = str(imdb_id or "").strip().lower()
    if text.startswith("tt"):
        digits = text[2:]
    else:
        digits = text
    if not digits.isdigit():
        return None
    return f"https://www.imdb.com/title/tt{digits.zfill(7)}/"


def _extract_imdb_urls_from_html(html: str, max_results: int) -> list[str]:
    urls: list[str] = []
    seen_ids: set[str] = set()

    sources = [str(html or "")]
    sources.append(html_lib.unescape(sources[0]).replace("\\/", "/"))

    def _push(imdb_id: str) -> None:
        clean_id = imdb_id.lower()
        if clean_id in seen_ids:
            return
        url = _imdb_id_url(clean_id)
        if not url:
            return
        seen_ids.add(clean_id)
        urls.append(url)

    for source in sources:
        for match in IMDB_ID_FROM_HTML.findall(source):
            _push(match)
            if len(urls) >= max_results:
                return urls

    soup = BeautifulSoup(sources[-1], "html.parser")
    for link in soup.find_all("a", href=True):
        imdb_id = extract_imdb_id(str(link.get("href") or ""))
        if imdb_id:
            _push(imdb_id)
            if len(urls) >= max_results:
                return urls

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


def _find_best_imdb_url_cinemagoer(
    search_terms: list[str],
    max_results: int,
) -> tuple[str | None, str | None, bool]:
    if CinemagoerIMDb is None:
        return None, None, False

    client = CinemagoerIMDb()
    saw_candidates = False
    for term in search_terms:
        results = client.search_movie(term, results=max_results)
        if results:
            saw_candidates = True

        for candidate in results:
            movie_id = getattr(candidate, "movieID", None)
            url = _imdb_id_url(str(movie_id or ""))
            if url:
                return url, term, saw_candidates

    return None, None, saw_candidates


def _search_single_title(title: str, team: list[str], max_results: int) -> dict[str, Any]:
    search_terms = _build_search_terms(title, team)
    google_queries = _build_google_queries(search_terms)
    query_trace = (" | ".join(google_queries) or search_terms[0]) if search_terms else ""

    if not search_terms:
        return {
            "status": "error",
            "query": "",
            "error": "Not enough metadata",
        }

    google_error: str | None = None
    imdb_find_error: str | None = None
    cinemagoer_error: str | None = None
    google_saw_results = False
    imdb_find_saw_candidates = False
    cinemagoer_saw_candidates = False

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
        return {
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
        return {
            "status": "found",
            "query": f"imdb-find:{used_imdb_find_term or search_terms[0]}",
            "imdb_url": best_url,
            "source": "imdb-find",
        }

    try:
        best_url, used_cinemagoer_term, cinemagoer_saw_candidates = (
            _find_best_imdb_url_cinemagoer(search_terms, max_results=max_results)
        )
    except Exception as exc:
        best_url = None
        used_cinemagoer_term = None
        cinemagoer_error = str(exc)

    if best_url:
        return {
            "status": "found",
            "query": f"cinemagoer:{used_cinemagoer_term or search_terms[0]}",
            "imdb_url": best_url,
            "source": "cinemagoer",
        }

    first_query = google_queries[0] if google_queries else search_terms[0]
    errors = [message for message in [google_error, imdb_find_error, cinemagoer_error] if message]
    if errors and not google_saw_results and not imdb_find_saw_candidates and not cinemagoer_saw_candidates:
        return {
            "status": "error",
            "query": query_trace or first_query,
            "error": " ; ".join(errors),
        }

    return {
        "status": "not_found",
        "query": query_trace or first_query,
        "error": "No IMDb URL found after trying Google, IMDb find and Cinemagoer",
    }


def _search_and_store(movie_row: dict[str, Any], max_results: int) -> dict[str, Any]:
    movie_id = movie_row["id"]
    raw_title = movie_row.get("manual_title") or movie_row.get("extraction_title")
    raw_team_values = [str(item or "").strip() for item in (movie_row.get("manual_team") or movie_row.get("extraction_team") or [])]
    raw_team_values = [item for item in raw_team_values if item]

    titles = _split_titles(raw_title)
    if not titles:
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

    team_per_title = _team_segments_for_titles(raw_team_values, len(titles))

    item_results: list[dict[str, Any]] = []
    for index, title in enumerate(titles):
        team = team_per_title[index] if index < len(team_per_title) else []
        single = _search_single_title(title, team, max_results=max_results)
        single["title_index"] = index
        single["title_input"] = title
        item_results.append(single)

    urls = [str(item.get("imdb_url") or "").strip() for item in item_results if item.get("status") == "found"]
    queries = [str(item.get("query") or "").strip() for item in item_results if str(item.get("query") or "").strip()]

    if len(urls) == len(titles):
        imdb_query = queries[0] if len(queries) == 1 else join_values(queries)
        imdb_url = urls[0] if len(urls) == 1 else join_values(urls)

        movies.update_imdb(
            movie_id,
            imdb_query=imdb_query,
            imdb_url=imdb_url,
            imdb_status="found",
            imdb_last_error=None,
        )
        return {
            "id": movie_id,
            "status": "found",
            "query": imdb_query,
            "imdb_url": imdb_url,
            "items": item_results,
        }

    failed_items = [item for item in item_results if item.get("status") != "found"]
    status = "error" if any(item.get("status") == "error" for item in failed_items) else "not_found"
    fallback_query = join_values(queries) or join_values(titles)
    error_parts = [
        f"[{int(item.get('title_index', 0)) + 1}] {item.get('title_input')}: {item.get('error') or item.get('status')}"
        for item in failed_items
    ]

    movies.update_imdb(
        movie_id,
        imdb_query=fallback_query,
        imdb_url=None,
        imdb_status=status,
        imdb_last_error=" | ".join(error_parts),
    )

    return {
        "id": movie_id,
        "status": status,
        "query": fallback_query,
        "error": " | ".join(error_parts),
        "items": item_results,
    }


def run_batch(
    *,
    limit: int,
    overwrite: bool = False,
    movie_id: str | None = None,
    max_results: int = IMDB_MAX_RESULTS,
    sleep_seconds: float = IMDB_SLEEP_SECONDS,
) -> dict[str, Any]:
    results: list[dict[str, Any]] = []
    if movie_id:
        targets = [movie_id]
    else:
        targets = [row["id"] for row in movies.movies_for_imdb(limit=limit, overwrite=overwrite)]

    for mid in targets:
        result = search_one(
            mid,
            max_results=max_results,
            overwrite=overwrite,
        )
        results.append(result)
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)

    return {
        "requested": len(targets),
        "processed": len(results),
        "items": results,
    }


def _imdb_links_complete(movie: dict[str, Any]) -> bool:
    raw_title = movie.get("manual_title") or movie.get("extraction_title")
    titles = _split_titles(raw_title)
    urls = split_values(movie.get("imdb_url"))
    if not urls:
        return False
    if len(titles) <= 1:
        return True
    return len(urls) == len(titles)


def search_one(
    movie_id: str,
    *,
    max_results: int = IMDB_MAX_RESULTS,
    overwrite: bool = False,
) -> dict[str, Any]:
    movie = movies.get_movie(movie_id)
    if not movie:
        return {"id": movie_id, "status": "error", "error": "Película no encontrada"}

    if not overwrite and _imdb_links_complete(movie):
        return {
            "id": movie_id,
            "status": "skipped",
            "imdb_url": movie.get("imdb_url"),
            "query": movie.get("imdb_query"),
        }

    return _search_and_store(movie, max_results=max_results)
