import base64
from io import BytesIO
from pathlib import Path
from typing import Any

from PIL import Image

from ..clients import ClientError, ollama_chat
from ..config import VISION_TEAM_MODEL, VISION_TITLE_MODEL
from ..normalizers import parse_team_text
from . import movies

try:
    from pillow_heif import register_heif_opener

    register_heif_opener()
except Exception:  # pragma: no cover
    pass

PROMPT_TITLE = (
    "Esta es la portada de una pelicula. Puede tener estilos de letra creativos, "
    "efectos visuales o maquetaciones no convencionales. "
    "Si identificas el titulo con claridad, responde solo el titulo exacto. "
    "Si no estas seguro, responde solo: NO IDENTIFICADO."
)

PROMPT_TEAM = (
    "Estas viendo la portada de una pelicula. "
    "Extrae nombres de personas claramente identificables como director, productor o actor/actriz. "
    "Si no puedes identificar roles con certeza, incluye los nombres al final. "
    "Responde solo con una lista separada por comas, o cadena vacia si no hay nombres."
)


def _image_to_base64_jpeg(path: str, max_size: int = 1024) -> str:
    image_path = Path(path)
    with Image.open(image_path) as image:
        image = image.convert("RGB")
        if max(image.size) > max_size:
            image.thumbnail((max_size, max_size), Image.LANCZOS)

        buffer = BytesIO()
        image.save(buffer, format="JPEG")
        return base64.b64encode(buffer.getvalue()).decode("utf-8")


def extract_from_cover(
    image_path: str,
    *,
    title_model: str = VISION_TITLE_MODEL,
    team_model: str = VISION_TEAM_MODEL,
) -> dict[str, Any]:
    encoded = _image_to_base64_jpeg(image_path)

    title_raw = ollama_chat(
        model=title_model,
        messages=[
            {
                "role": "user",
                "content": PROMPT_TITLE,
                "images": [encoded],
            }
        ],
    )

    team_raw = ollama_chat(
        model=team_model,
        messages=[
            {
                "role": "user",
                "content": PROMPT_TEAM,
                "images": [encoded],
            }
        ],
    )

    clean_title = title_raw.strip().strip('"').strip()
    if clean_title.upper().startswith("NO IDENTIFICADO"):
        clean_title = "NO IDENTIFICADO"

    team = parse_team_text(team_raw)

    return {
        "title": clean_title,
        "team": team,
        "title_raw": title_raw,
        "team_raw": team_raw,
        "title_model": title_model,
        "team_model": team_model,
    }


def run_batch(
    *,
    limit: int,
    overwrite: bool = False,
    movie_id: str | None = None,
    title_model: str = VISION_TITLE_MODEL,
    team_model: str = VISION_TEAM_MODEL,
) -> dict[str, Any]:
    if movie_id:
        movie = movies.get_movie(movie_id)
        targets = [] if movie is None else [{"id": movie_id, "image_path": movie["image_path"]}]
    else:
        targets = movies.movies_for_extraction(limit=limit, overwrite=overwrite)

    processed: list[dict[str, Any]] = []

    for row in targets:
        mid = row["id"]
        image_path = row["image_path"]

        try:
            payload = extract_from_cover(
                image_path,
                title_model=title_model,
                team_model=team_model,
            )
            movies.update_extraction(
                mid,
                title=payload["title"],
                team=payload["team"],
                title_raw=payload["title_raw"],
                team_raw=payload["team_raw"],
                title_model=payload["title_model"],
                team_model=payload["team_model"],
            )
            processed.append(
                {
                    "id": mid,
                    "status": "ok",
                    "title": payload["title"],
                    "team": payload["team"],
                }
            )
        except (ClientError, FileNotFoundError, OSError, ValueError) as exc:
            processed.append({"id": mid, "status": "error", "error": str(exc)})

    return {
        "requested": len(targets),
        "processed": len(processed),
        "items": processed,
    }
