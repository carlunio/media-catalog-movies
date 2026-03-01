from typing import Iterable

MULTI_SEPARATOR = ";"
PLOT_MULTI_SEPARATOR = ";\n"


def split_values(value: str | None, *, separator: str = MULTI_SEPARATOR, keep_empty: bool = False) -> list[str]:
    text = str(value or "").strip()
    if not text:
        return []

    parts = [part.strip() for part in text.split(separator)]
    if keep_empty:
        return parts
    return [part for part in parts if part]


def join_values(
    values: Iterable[str | None],
    *,
    separator: str = MULTI_SEPARATOR,
    keep_empty: bool = False,
) -> str:
    cleaned: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if text or keep_empty:
            cleaned.append(text)

    if not cleaned:
        return ""
    if keep_empty and not any(cleaned):
        return ""
    return separator.join(cleaned)
