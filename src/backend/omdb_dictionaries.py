import json
from functools import lru_cache
from pathlib import Path
from typing import Any

from .config import PROJECT_ROOT
from .multi_value import MULTI_SEPARATOR, join_values, split_values

DICT_FILE_NAME = "dicts.json"
FIELD_DICTIONARIES = {
    "Genre": "genre",
    "Country": "country",
    "Language": "language",
    "Type": "type",
    "omdb_genre": "genre",
    "omdb_country": "country",
    "omdb_language": "language",
    "omdb_type": "type",
    "genres": "genre",
    "country": "country",
    "languages": "language",
    "item_type": "type",
}


def _candidate_dict_paths() -> list[Path]:
    repo_root = Path(__file__).resolve().parents[2]
    return [
        PROJECT_ROOT / "assets" / DICT_FILE_NAME,
        repo_root / "assets" / DICT_FILE_NAME,
    ]


@lru_cache(maxsize=1)
def load_omdb_dictionaries() -> dict[str, dict[str, str]]:
    for path in _candidate_dict_paths():
        if path.exists():
            raw = json.loads(path.read_text(encoding="utf-8"))
            return {
                str(section): {str(key): str(value) for key, value in values.items()}
                for section, values in raw.items()
                if isinstance(values, dict)
            }
    return {}


def _translate_comma_list(value: str, dictionary: dict[str, str]) -> str:
    parts = [part.strip() for part in value.split(",")]
    translated = [dictionary.get(part, part) for part in parts if part]
    return ", ".join(translated)


def translate_omdb_value(value: Any, dictionary_name: str | None) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    if not dictionary_name:
        return text

    dictionary = load_omdb_dictionaries().get(dictionary_name, {})
    if not dictionary:
        return text

    groups = split_values(text, separator=MULTI_SEPARATOR, keep_empty=True)
    if not groups:
        return None

    translated_groups = [
        _translate_comma_list(group, dictionary) if group.strip() else ""
        for group in groups
    ]
    translated = join_values(
        translated_groups,
        separator=MULTI_SEPARATOR,
        keep_empty=True,
    )
    return translated or None


def translate_omdb_field(value: Any, field_name: str) -> str | None:
    return translate_omdb_value(value, FIELD_DICTIONARIES.get(field_name))


def translate_omdb_fields(fields: dict[str, Any]) -> dict[str, Any]:
    translated = dict(fields)
    for field_name in FIELD_DICTIONARIES:
        if field_name in translated:
            translated[field_name] = translate_omdb_field(translated[field_name], field_name)
    return translated
