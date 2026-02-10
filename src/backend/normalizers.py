import json
import re
from pathlib import Path
from typing import Any

IMDB_ID_PATTERN = re.compile(r"(tt\d{7,8})", re.IGNORECASE)
IMDB_ID_EXACT_PATTERN = re.compile(r"tt\d{7,8}", re.IGNORECASE)
IMDB_URL_PATTERN = re.compile(
    r"https?://(?:www\.|m\.)?imdb\.com/(?:[a-z]{2}(?:-[a-z]{2})?/)?title/(tt\d{7,8})(?:[/?#].*)?$",
    re.IGNORECASE,
)


def parse_json_list(value: Any) -> list[str]:
    if value is None:
        return []

    if isinstance(value, list):
        return [str(v).strip() for v in value if str(v).strip()]

    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []

        try:
            loaded = json.loads(text)
            if isinstance(loaded, list):
                return [str(v).strip() for v in loaded if str(v).strip()]
        except json.JSONDecodeError:
            pass

        return [part.strip() for part in text.split(",") if part.strip()]

    return []


def parse_team_text(value: str) -> list[str]:
    if not value:
        return []
    normalized = value.replace("\n", ",")
    return [part.strip() for part in normalized.split(",") if part.strip()]


def extract_imdb_id(text: str | None) -> str | None:
    if not text:
        return None
    match = IMDB_ID_PATTERN.search(text)
    return match.group(1).lower() if match else None


def canonical_imdb_url(text: str | None) -> str | None:
    if not text:
        return None

    candidate = text.strip()
    if not candidate:
        return None

    # Accept a direct IMDb ID (e.g. "tt5816682")
    if IMDB_ID_EXACT_PATTERN.fullmatch(candidate):
        return f"https://www.imdb.com/title/{candidate.lower()}/"

    # Accept URLs typed without scheme (e.g. "www.imdb.com/title/tt...")
    lowered = candidate.lower()
    if lowered.startswith("www.imdb.com") or lowered.startswith("imdb.com") or lowered.startswith("m.imdb.com"):
        candidate = f"https://{candidate}"

    match = IMDB_URL_PATTERN.search(candidate)
    if not match:
        return None
    imdb_id = match.group(1).lower()
    return f"https://www.imdb.com/title/{imdb_id}/"


def ensure_abs_path(path: str | Path) -> str:
    return str(Path(path).expanduser().resolve())
