import re
from typing import Any

import requests
from bs4 import BeautifulSoup

from ..config import REQUEST_TIMEOUT_SECONDS
from ..multi_value import join_values, split_values
from . import movies

YEAR_SUFFIX_RE = re.compile(r"\s*\(\d{4}\)$")

HEADERS = {
    "Accept-Language": "es-ES,es;q=0.9",
    "User-Agent": "Mozilla/5.0",
}


def _extract_title_es_from_html(html_text: str) -> str:
    soup = BeautifulSoup(str(html_text or ""), "html.parser")
    title_tag = soup.find("title")
    if title_tag is None:
        raise ValueError("No se pudo extraer el título en español: falta <title>")

    raw_title = title_tag.get_text(strip=True)
    if " - " in raw_title:
        raw_title = raw_title.split(" - ", 1)[0]

    title_es = YEAR_SUFFIX_RE.sub("", raw_title).strip()
    if not title_es:
        raise ValueError("Título en español vacío tras normalizar")
    return title_es


def fetch_title_es(imdb_url: str) -> str:
    response = requests.get(imdb_url, headers=HEADERS, timeout=REQUEST_TIMEOUT_SECONDS)
    # IMDb may return 202 in anti-bot flows; still try parsing HTML title.
    if response.status_code not in (200, 202):
        raise Exception(f"Error al acceder a IMDb: {response.status_code}")

    try:
        return _extract_title_es_from_html(response.text)
    except Exception as exc:
        if response.status_code == 202:
            raise Exception(
                "IMDb devolvio 202 y no se pudo extraer <title> (posible bloqueo temporal)"
            ) from exc
        raise


def fetch_one(movie_id: str, *, imdb_url: str | None = None) -> dict[str, Any]:
    movie = movies.get_movie(movie_id)
    if movie is None:
        return {"id": movie_id, "status": "error", "error": "Película no encontrada"}

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
