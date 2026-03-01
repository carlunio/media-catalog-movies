from typing import Any

from ..clients import ClientError, ollama_chat
from ..config import TRANSLATION_MODEL
from ..multi_value import PLOT_MULTI_SEPARATOR, join_values, split_values
from . import movies

SYSTEM_PROMPT = (
    "You are an English to Spanish translation tool. "
    "Translate movie plot text to neutral Spanish. "
    "Return only the translated text."
)


def translate_plot(plot_en: str, model: str) -> str:
    return ollama_chat(
        model=model,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": plot_en},
        ],
    )


def _translate_multi_plot(plot_en: str, model: str) -> str:
    parts = split_values(plot_en, separator=PLOT_MULTI_SEPARATOR)
    if len(parts) <= 1:
        return translate_plot(plot_en, model=model)

    translated_parts: list[str] = []
    for part in parts:
        translated_parts.append(translate_plot(part, model=model).strip())

    return join_values(translated_parts, separator=PLOT_MULTI_SEPARATOR, keep_empty=True)


def run_batch(
    *,
    limit: int,
    overwrite: bool = False,
    movie_id: str | None = None,
    model: str = TRANSLATION_MODEL,
) -> dict[str, Any]:
    if movie_id:
        movie = movies.get_movie(movie_id)
        targets = []
        if movie and movie.get("omdb_plot_en"):
            targets = [{"id": movie_id, "omdb_plot_en": movie["omdb_plot_en"]}]
    else:
        targets = movies.movies_for_translation(limit=limit, overwrite=overwrite)

    items: list[dict[str, Any]] = []
    for row in targets:
        mid = row["id"]
        plot_en = row["omdb_plot_en"]

        try:
            translated = _translate_multi_plot(plot_en, model=model)
            movies.update_plot_translation(
                mid,
                plot_es=translated,
                status="translated",
                error=None,
            )
            items.append({"id": mid, "status": "translated"})
        except (ClientError, RuntimeError, ValueError) as exc:
            movies.update_plot_translation(
                mid,
                plot_es=None,
                status="error",
                error=str(exc),
            )
            items.append({"id": mid, "status": "error", "error": str(exc)})

    return {
        "requested": len(targets),
        "processed": len(items),
        "items": items,
    }
