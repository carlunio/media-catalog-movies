import csv
from datetime import datetime
from pathlib import Path
import shutil
from typing import Any
from urllib.parse import urlparse

import requests

from ..config import EXPORTS_DIR, PROJECT_ROOT, REQUEST_TIMEOUT_SECONDS
from ..database import get_connection
from . import catalog

OMDB_SECOND_IMAGE_OUTPUT_DIR = PROJECT_ROOT / "data/output/covers"
EXPORT_COVERS_DIR = EXPORTS_DIR / "covers"
IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp", ".gif"}
POSTER_USER_AGENT = "Mozilla/5.0 (compatible; MediaCatalogMovies/1.0)"


class ExportValidationError(ValueError):
    def __init__(self, validation: dict[str, Any]):
        super().__init__("Hay fichas con errores de validación para exportar.")
        self.validation = validation


def _normalize_ids(ids: list[str] | tuple[str, ...] | None) -> list[str]:
    normalized: list[str] = []
    for raw_id in ids or []:
        item_id = str(raw_id or "").strip()
        if item_id and item_id not in normalized:
            normalized.append(item_id)
    return normalized


def query_export_rows(
    ids: list[str] | tuple[str, ...] | None = None,
) -> tuple[list[str], list[dict[str, Any]]]:
    normalized_ids = _normalize_ids(ids)
    sql = f'SELECT * FROM "{catalog.EXPORT_VIEW_NAME}"'
    params: list[Any] = []
    if ids is not None and not normalized_ids:
        sql += " WHERE FALSE"
    elif normalized_ids:
        placeholders = ", ".join(["?"] * len(normalized_ids))
        sql += f' WHERE "{catalog.EXPORT_REFERENCE_COLUMN}" IN ({placeholders})'
        params.extend(normalized_ids)
    sql += f' ORDER BY "{catalog.EXPORT_REFERENCE_COLUMN}"'

    with get_connection() as con:
        cursor = con.execute(sql, params)
        columns = [str(description[0]) for description in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
    return columns, rows


def _serialize_value(value: Any) -> str:
    if value is None:
        return ""
    return (
        str(value)
        .replace("#", " ")
        .replace('"', "'")
        .replace("\t", " ")
        .replace("\r\n", " / ")
        .replace("\r", " / ")
        .replace("\n", " / ")
    )


def _is_blank(value: Any) -> bool:
    return not str(value or "").strip()


def _validate_price(value: Any) -> bool:
    try:
        return value is not None and float(value) > 0
    except (TypeError, ValueError):
        return False


def _validate_image_path(value: Any) -> bool:
    return _resolved_local_path(str(value or "")) is not None


def _candidate_items_for_validation(
    ids: list[str] | tuple[str, ...] | None = None,
) -> list[dict[str, Any]]:
    normalized_ids = _normalize_ids(ids)
    params: list[Any] = []
    where_sql = ""
    if ids is not None:
        if not normalized_ids:
            return []
        placeholders = ", ".join(["?"] * len(normalized_ids))
        where_sql = f"WHERE id IN ({placeholders})"
        params.extend(normalized_ids)
    else:
        placeholders = ", ".join(["?"] * len(catalog.EXPORTABLE_LISTING_STATUSES))
        where_sql = f"WHERE listing_status IN ({placeholders})"
        params.extend(catalog.EXPORTABLE_LISTING_STATUSES)

    with get_connection() as con:
        cursor = con.execute(
            f"""
            SELECT id, title, sale_price, listing_status, tc_section,
                   tc_condition, image_path
            FROM {catalog.ITEMS_TABLE}
            {where_sql}
            ORDER BY LOWER(id), id
            """,
            params,
        )
        columns = [str(description[0]) for description in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


def _validation_row(item: dict[str, Any]) -> dict[str, Any]:
    errors: list[str] = []
    item_id = str(item.get("id") or "").strip()
    listing_status = str(item.get("listing_status") or "").strip()

    if _is_blank(item.get("title")):
        errors.append("Falta el título.")
    if not _validate_price(item.get("sale_price")):
        errors.append("Falta un precio mayor que cero.")
    if listing_status not in catalog.EXPORTABLE_LISTING_STATUSES:
        errors.append("La operación debe ser ALTA, CAMBIO o BAJA.")
    if _is_blank(item.get("tc_section")):
        errors.append("Falta la sección de Todocolección.")
    if _is_blank(item.get("tc_condition")):
        errors.append("Falta el estado de Todocolección.")
    if not _validate_image_path(item.get("image_path")):
        errors.append("Falta una carátula local existente.")

    return {
        "id": item_id,
        "title": item.get("title"),
        "listing_status": listing_status or None,
        "sale_price": item.get("sale_price"),
        "tc_section": item.get("tc_section"),
        "tc_condition": item.get("tc_condition"),
        "image_path": item.get("image_path"),
        "is_valid": not errors,
        "errors": errors,
    }


def validate_export_items(
    ids: list[str] | tuple[str, ...] | None = None,
) -> dict[str, Any]:
    normalized_ids = _normalize_ids(ids)
    items = _candidate_items_for_validation(ids=ids)
    rows = [_validation_row(item) for item in items]

    if ids is not None:
        found_ids = {str(item.get("id") or "").strip() for item in items}
        for item_id in normalized_ids:
            if item_id not in found_ids:
                rows.append(
                    {
                        "id": item_id,
                        "title": None,
                        "listing_status": None,
                        "sale_price": None,
                        "tc_section": None,
                        "tc_condition": None,
                        "image_path": None,
                        "is_valid": False,
                        "errors": ["La ficha no existe."],
                    }
                )

    valid_ids = [str(row["id"]) for row in rows if row.get("is_valid")]
    invalid_rows = [row for row in rows if not row.get("is_valid")]
    return {
        "ok": True,
        "rows": rows,
        "rows_count": len(rows),
        "valid_count": len(valid_ids),
        "invalid_count": len(invalid_rows),
        "valid_ids": valid_ids,
        "invalid_ids": [str(row["id"]) for row in invalid_rows],
        "ids": [str(row["id"]) for row in rows],
    }


def get_export_preview(
    ids: list[str] | tuple[str, ...] | None = None,
) -> dict[str, Any]:
    columns, rows = query_export_rows(ids=ids)
    exported_ids = [
        str(row.get(catalog.EXPORT_REFERENCE_COLUMN) or "").strip()
        for row in rows
        if str(row.get(catalog.EXPORT_REFERENCE_COLUMN) or "").strip()
    ]
    return {
        "columns": columns,
        "rows": rows,
        "ids": exported_ids,
        "rows_count": len(rows),
        "validation": validate_export_items(ids=ids),
    }


def export_movies_csv(
    output_path: Path | None = None,
    *,
    ids: list[str] | tuple[str, ...] | None = None,
) -> dict[str, Any]:
    validation = validate_export_items(ids=ids)
    preview = get_export_preview(ids=ids)
    columns = list(preview["columns"])
    rows = list(preview["rows"])
    if output_path is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = EXPORTS_DIR / f"peliculas_{timestamp}.csv"

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=columns,
            delimiter="#",
            quoting=csv.QUOTE_NONE,
            extrasaction="ignore",
            lineterminator="\n",
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {column: _serialize_value(row.get(column)) for column in columns}
            )

    return {
        "path": output_path,
        "filename": output_path.name,
        "rows": len(rows),
        "columns": columns,
        "ids": list(preview["ids"]),
        "validation": validation,
    }


def clear_exported_items_listing_status(ids: list[str]) -> dict[str, Any]:
    normalized_ids = _normalize_ids(ids)
    if not normalized_ids:
        return {"updated": 0, "ids": []}

    placeholders = ", ".join(["?"] * len(normalized_ids))
    eligible_placeholders = ", ".join(["?"] * len(catalog.EXPORTABLE_LISTING_STATUSES))
    params = normalized_ids + list(catalog.EXPORTABLE_LISTING_STATUSES)
    with get_connection() as con:
        rows = con.execute(
            f"""
            SELECT id
            FROM {catalog.ITEMS_TABLE}
            WHERE id IN ({placeholders})
              AND listing_status IN ({eligible_placeholders})
            """,
            params,
        ).fetchall()
        matched_ids = [
            str(row[0] or "").strip() for row in rows if str(row[0] or "").strip()
        ]
        if matched_ids:
            matched_placeholders = ", ".join(["?"] * len(matched_ids))
            con.execute(
                f"""
                UPDATE {catalog.ITEMS_TABLE}
                SET listing_status = NULL, updated_at = CURRENT_TIMESTAMP
                WHERE id IN ({matched_placeholders})
                """,
                matched_ids,
            )
    return {"updated": len(matched_ids), "ids": matched_ids}


def _resolved_local_path(raw_path: str | None) -> Path | None:
    text = str(raw_path or "").strip()
    if not text:
        return None
    path = Path(text).expanduser()
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    resolved = path.resolve()
    return resolved if resolved.exists() and resolved.is_file() else None


def _export_cover_filename(item_id: str, source: Path) -> str:
    extension = source.suffix.lower()
    if extension not in IMAGE_EXTENSIONS:
        extension = ".jpg"
    return f"{item_id}{extension}"


def export_cover_images(
    ids: list[str] | tuple[str, ...] | None = None,
    *,
    output_dir: Path | None = None,
) -> dict[str, Any]:
    preview = get_export_preview(ids=ids)
    selected_ids = list(preview["ids"])
    target_dir = (output_dir or EXPORT_COVERS_DIR).resolve()
    target_dir.mkdir(parents=True, exist_ok=True)

    if not selected_ids:
        return {
            "covers_dir": str(target_dir),
            "ids": [],
            "copied": [],
            "missing": [],
            "failed": [],
            "copied_count": 0,
            "missing_count": 0,
            "failed_count": 0,
        }

    placeholders = ", ".join(["?"] * len(selected_ids))
    with get_connection() as con:
        rows = con.execute(
            f"""
            SELECT id, image_path
            FROM {catalog.ITEMS_TABLE}
            WHERE id IN ({placeholders})
            """,
            selected_ids,
        ).fetchall()
    image_paths = {str(row[0]): row[1] for row in rows}

    copied: list[dict[str, str]] = []
    missing: list[str] = []
    failed: list[dict[str, str]] = []
    for item_id in selected_ids:
        source = _resolved_local_path(image_paths.get(item_id))
        if source is None:
            missing.append(item_id)
            continue
        destination = target_dir / _export_cover_filename(item_id, source)
        try:
            shutil.copy2(source, destination)
        except Exception as exc:
            failed.append({"id": item_id, "source": str(source), "error": str(exc)})
            continue
        copied.append({"id": item_id, "source": str(source), "path": str(destination)})

    return {
        "covers_dir": str(target_dir),
        "ids": selected_ids,
        "copied": copied,
        "missing": missing,
        "failed": failed,
        "copied_count": len(copied),
        "missing_count": len(missing),
        "failed_count": len(failed),
    }


def _normalize_poster_url(raw_url: str | None) -> str | None:
    url = str(raw_url or "").strip()
    if not url or url.upper() == "N/A":
        return None
    parsed = urlparse(url)
    if parsed.scheme.lower() not in {"http", "https"} or not parsed.netloc:
        return None
    return url


def _split_semicolon_keep_empty(raw_value: str | None) -> list[str]:
    text = str(raw_value or "")
    parts = [part.strip() for part in text.split(";")]
    if len(parts) == 1 and not parts[0]:
        return []
    return parts


def _extract_poster_slots(raw_value: str | None) -> list[tuple[int, str | None]]:
    return [
        (index, _normalize_poster_url(part))
        for index, part in enumerate(_split_semicolon_keep_empty(raw_value), start=1)
    ]


def _download_poster(url: str) -> bytes:
    response = requests.get(
        url,
        timeout=REQUEST_TIMEOUT_SECONDS,
        headers={"User-Agent": POSTER_USER_AGENT},
    )
    response.raise_for_status()
    content = bytes(response.content or b"")
    if not content:
        raise ValueError("La respuesta del póster está vacía")
    return content


def _second_image_filename(movie_id: str) -> str:
    return f"{movie_id}_2.jpg"


def _select_poster_url(
    raw_poster: str | None, *, poster_slot: int | None = None
) -> tuple[int | None, str | None]:
    poster_slots = _extract_poster_slots(raw_poster)
    if poster_slot is not None:
        for index, poster_url in poster_slots:
            if index == poster_slot:
                return index, poster_url
        return None, None

    for index, poster_url in poster_slots:
        if poster_url:
            return index, poster_url
    return None, None


def _omdb_poster_rows(ids: list[str] | tuple[str, ...] | None = None) -> list[tuple[Any, Any]]:
    normalized_ids = _normalize_ids(ids)
    if ids is not None and not normalized_ids:
        return []

    params: list[Any] = []
    id_filter = ""
    if normalized_ids:
        placeholders = ", ".join(["?"] * len(normalized_ids))
        id_filter = f"AND id IN ({placeholders})"
        params.extend(normalized_ids)

    with get_connection() as con:
        return con.execute(
            f"""
            SELECT id, omdb_poster
            FROM movies
            WHERE omdb_poster IS NOT NULL
              AND TRIM(omdb_poster) <> ''
              {id_filter}
            ORDER BY LOWER(id), id
            """,
            params,
        ).fetchall()


def download_omdb_second_images(
    ids: list[str] | tuple[str, ...] | None = None,
    *,
    poster_slot: int | None = None,
    output_dir: Path | None = None,
) -> dict[str, Any]:
    target_dir = (output_dir or OMDB_SECOND_IMAGE_OUTPUT_DIR).resolve()
    target_dir.mkdir(parents=True, exist_ok=True)
    rows = _omdb_poster_rows(ids=ids)

    downloaded: list[dict[str, Any]] = []
    skipped: list[dict[str, str]] = []
    errors: list[dict[str, str]] = []
    for raw_movie_id, raw_poster in rows:
        movie_id = str(raw_movie_id or "").strip()
        if not movie_id:
            continue

        selected_slot, poster_url = _select_poster_url(raw_poster, poster_slot=poster_slot)
        if not poster_url:
            reason = "No hay URL válida en omdb_poster"
            if poster_slot is not None:
                reason = f"No hay URL válida en la posición {poster_slot:02d} de omdb_poster"
            skipped.append({"id": movie_id, "reason": reason})
            continue

        file_path = target_dir / _second_image_filename(movie_id)
        tmp_path = file_path.with_name(f"{file_path.name}.tmp")
        try:
            tmp_path.write_bytes(_download_poster(poster_url))
            tmp_path.replace(file_path)
        except Exception as exc:
            if tmp_path.exists():
                tmp_path.unlink()
            errors.append({"id": movie_id, "url": poster_url, "error": str(exc)})
            continue

        downloaded.append(
            {
                "id": movie_id,
                "slot": selected_slot,
                "url": poster_url,
                "path": str(file_path),
            }
        )

    return {
        "ok": True,
        "output_dir": str(target_dir),
        "covers_dir": str(target_dir),
        "requested_movies": len(rows),
        "downloaded_movies": len(downloaded),
        "downloaded_files": len(downloaded),
        "downloaded_count": len(downloaded),
        "skipped_movies": len(skipped),
        "skipped_count": len(skipped),
        "error_movies": len(errors),
        "failed_count": len(errors),
        "downloaded": downloaded,
        "skipped": skipped,
        "errors": errors,
        "failed": errors,
    }


def export_original_covers(output_dir: Path | None = None) -> dict[str, Any]:
    return download_omdb_second_images(output_dir=output_dir)
