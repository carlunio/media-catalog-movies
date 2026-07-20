import json
import re
from typing import Any

import requests
from bs4 import BeautifulSoup

from ..config import REQUEST_TIMEOUT_SECONDS
from ..multi_value import join_values, split_values
from ..normalizers import extract_imdb_id
from . import movies

YEAR_SUFFIX_RE = re.compile(r"\s*\(\d{4}\)$")
IMDB_SUFFIX_RE = re.compile(r"\s*-\s*IMDb\s*$", re.IGNORECASE)
IMDB_BLOCK_MARKERS = (
    "verify that you're not a robot",
    "javascript is disabled",
    "enable javascript and then reload",
)

HEADERS = {
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.5",
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
}


def _clean_title(value: Any) -> str:
    title = " ".join(str(value or "").strip().split())
    if not title:
        return ""
    title = IMDB_SUFFIX_RE.sub("", title).strip()
    title = YEAR_SUFFIX_RE.sub("", title).strip()
    if " - " in title:
        title = title.split(" - ", 1)[0].strip()
    return title


def _candidate_title_urls(imdb_url: str) -> list[str]:
    imdb_id = extract_imdb_id(imdb_url)
    urls: list[str] = []
    if imdb_id:
        urls.append(f"https://www.imdb.com/es-es/title/{imdb_id}/")
        urls.append(f"https://www.imdb.com/title/{imdb_id}/")
    if imdb_url and imdb_url not in urls:
        urls.append(imdb_url)
    return urls


def _titles_from_json_ld(soup: BeautifulSoup) -> list[str]:
    titles: list[str] = []
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        raw = script.string or script.get_text(" ", strip=True)
        if not raw:
            continue
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            continue
        candidates = payload if isinstance(payload, list) else [payload]
        for item in candidates:
            if not isinstance(item, dict):
                continue
            for key in ("name", "alternateName"):
                clean = _clean_title(item.get(key))
                if clean:
                    titles.append(clean)
    return titles


def _titles_from_meta(soup: BeautifulSoup) -> list[str]:
    selectors = [
        {"property": "og:title"},
        {"name": "title"},
        {"name": "twitter:title"},
    ]
    titles: list[str] = []
    for attrs in selectors:
        tag = soup.find("meta", attrs=attrs)
        if tag is None:
            continue
        clean = _clean_title(tag.get("content"))
        if clean:
            titles.append(clean)
    return titles


def _extract_title_es_from_html(html_text: str) -> str:
    raw_html = str(html_text or "")
    lowered = raw_html.lower()
    if any(marker in lowered for marker in IMDB_BLOCK_MARKERS):
        raise ValueError("IMDb devolvió una verificación anti-bot sin título usable")

    soup = BeautifulSoup(raw_html, "html.parser")

    for candidate in [*_titles_from_json_ld(soup), *_titles_from_meta(soup)]:
        if candidate:
            return candidate

    h1 = soup.find("h1")
    if h1 is not None:
        clean = _clean_title(h1.get_text(" ", strip=True))
        if clean:
            return clean

    title_tag = soup.find("title")
    if title_tag is None:
        raise ValueError("No se pudo extraer el título en español: falta <title>")

    title_es = _clean_title(title_tag.get_text(strip=True))
    if not title_es:
        raise ValueError("Título en español vacío tras normalizar")
    return title_es


def fetch_title_es(imdb_url: str) -> str:
    errors: list[str] = []
    for target_url in _candidate_title_urls(imdb_url):
        try:
            response = requests.get(
                target_url,
                headers=HEADERS,
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
            # IMDb may return 202 in anti-bot flows; still try parsing HTML title.
            if response.status_code not in (200, 202):
                raise Exception(f"Error al acceder a IMDb: {response.status_code}")
            return _extract_title_es_from_html(response.text)
        except Exception as exc:
            errors.append(f"{target_url}: {exc}")

    raise Exception("No se pudo extraer el título ES de IMDb: " + " | ".join(errors))


def _title_matches_targets(title: str, target_urls: list[str]) -> bool:
    clean = str(title or "").strip()
    if not clean:
        return False
    if len(target_urls) <= 1:
        return True
    return len(split_values(clean)) == len(target_urls)


def _fallback_title(movie: dict[str, Any], target_urls: list[str]) -> tuple[str | None, str | None]:
    candidates = [
        (movie.get("imdb_title_es"), str(movie.get("imdb_title_es_status") or "fetched").strip() or "fetched"),
        (movie.get("manual_title"), "manual_title"),
        (movie.get("extraction_title"), "fallback"),
    ]
    for raw_title, status in candidates:
        title = str(raw_title or "").strip()
        if title and _title_matches_targets(title, target_urls):
            return title, status
    return None, None


def fetch_one(
    movie_id: str,
    *,
    imdb_url: str | None = None,
    overwrite: bool = False,
) -> dict[str, Any]:
    movie = movies.get_movie(movie_id)
    if movie is None:
        return {"id": movie_id, "status": "error", "error": "Película no encontrada"}

    if movies.has_manual_imdb_title_es(movie):
        return {
            "id": movie_id,
            "status": "skipped",
            "reason": "Título ES manual ya informado",
            "imdb_title_es": movie.get("imdb_title_es"),
        }

    if movies.resolve_imdb_title_es_from_manual_title(
        movie_id,
        imdb_url=imdb_url or movie.get("imdb_url"),
    ):
        refreshed = movies.get_movie(movie_id) or movie
        return {
            "id": movie_id,
            "status": "skipped",
            "reason": "Título ES resuelto por título manual",
            "imdb_title_es": refreshed.get("imdb_title_es"),
        }

    if not overwrite and movies.is_imdb_title_es_complete(movie):
        return {
            "id": movie_id,
            "status": "skipped",
            "reason": "Título ES ya resuelto",
            "imdb_title_es": movie.get("imdb_title_es"),
            "effective_title_es": movies.effective_spanish_title(movie),
        }

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
        fallback_title, fallback_status = _fallback_title(movie, target_urls)
        if fallback_title and fallback_status:
            movies.update_imdb_title_es(
                movie_id,
                title_es=fallback_title,
                status=fallback_status,
                error=None,
            )
            return {
                "id": movie_id,
                "status": fallback_status,
                "imdb_title_es": fallback_title,
                "warning": "IMDb no devolvió un título parseable; se usó un título local",
            }

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
        items.append(
            fetch_one(
                mid,
                imdb_url=str(row.get("imdb_url") or "").strip() or None,
                overwrite=overwrite,
            )
        )

    return {
        "requested": len(targets),
        "processed": len(items),
        "items": items,
    }
