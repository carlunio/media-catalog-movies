import re
from html import unescape
from typing import Any

import requests

from ..config import REQUEST_TIMEOUT_SECONDS
from ..multi_value import join_values, split_values
from . import movies

TITLE_TAG_RE = re.compile(r"<title[^>]*>(.*?)</title>", flags=re.IGNORECASE | re.DOTALL)
YEAR_SUFFIX_RE = re.compile(r"\s*\(\d{4}\)$")

HEADERS = {
    "Accept-Language": "es-ES,es;q=0.9",
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
}


def _extract_title_from_html(html_text: str) -> str:
    match = TITLE_TAG_RE.search(html_text or "")
    if not match:
        raise ValueError("No se encontro etiqueta <title> en IMDb")

    raw_title = unescape(match.group(1)).strip()
    if " - " in raw_title:
        raw_title = raw_title.split(" - ", 1)[0]
    raw_title = YEAR_SUFFIX_RE.sub("", raw_title).strip()
    if not raw_title:
        raise ValueError("Titulo vacio tras normalizar respuesta de IMDb")
    return raw_title


def fetch_title_es(imdb_url: str) -> str:
    response = requests.get(imdb_url, headers=HEADERS, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()
    return _extract_title_from_html(response.text)


def fetch_one(movie_id: str, *, imdb_url: str | None = None) -> dict[str, Any]:
    movie = movies.get_movie(movie_id)
    if movie is None:
        return {"id": movie_id, "status": "error", "error": "Movie not found"}

    target_urls = split_values(imdb_url or movie.get("imdb_url"))
    if not target_urls:
        movies.update_imdb_title_es(
            movie_id,
            title_es=None,
            status="error",
            error="Missing imdb_url",
        )
        return {"id": movie_id, "status": "error", "error": "Missing imdb_url"}

    titles: list[str] = []
    errors: list[str] = []
    for index, target_url in enumerate(target_urls, start=1):
        try:
            titles.append(fetch_title_es(target_url))
        except Exception as exc:
            errors.append(f"[{index}] {target_url}: {exc}")

    if errors:
        movies.update_imdb_title_es(
            movie_id,
            title_es=None,
            status="error",
            error=" | ".join(errors),
        )
        return {"id": movie_id, "status": "error", "error": " | ".join(errors)}

    title_es = titles[0] if len(titles) == 1 else join_values(titles)

    movies.update_imdb_title_es(
        movie_id,
        title_es=title_es,
        status="fetched",
        error=None,
    )
    return {"id": movie_id, "status": "fetched", "imdb_title_es": title_es}


def run_batch(
    *,
    limit: int,
    overwrite: bool = False,
    movie_id: str | None = None,
) -> dict[str, Any]:
    if movie_id:
        target = movies.get_movie(movie_id)
        targets = []
        if target:
            targets = [{"id": movie_id, "imdb_url": target.get("imdb_url")}]
    else:
        targets = movies.movies_for_imdb_title_es(limit=limit, overwrite=overwrite)

    items: list[dict[str, Any]] = []
    for row in targets:
        mid = str(row.get("id") or "").strip()
        if not mid:
            continue
        items.append(fetch_one(mid, imdb_url=str(row.get("imdb_url") or "").strip() or None))

    return {
        "requested": len(targets),
        "processed": len(items),
        "items": items,
    }
