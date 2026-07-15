from __future__ import annotations

from datetime import datetime
from pathlib import Path, PureWindowsPath
import shutil
from typing import Any

from ..config import DEFAULT_COVERS_DIR, PROJECT_ROOT

from ..omdb_dictionaries import translate_omdb_field

TABLE_NAME = "items"

COLUMNS = (
    "id",
    "title",
    "original_title",
    "item_type",
    "director",
    "writers",
    "actors",
    "year",
    "rated",
    "released",
    "runtime",
    "genres",
    "country",
    "languages",
    "plot",
    "awards",
    "production",
    "imdb_url",
    "imdb_rating",
    "imdb_votes",
    "box_office",
    "sale_price",
    "listing_status",
    "stock_status",
    "tc_section",
    "tc_condition",
    "condition_comments",
    "notes",
    "image_path",
    "updated_at",
)

EDITABLE_COLUMNS = set(COLUMNS) - {"id", "updated_at"}

SUMMARY_COLUMNS = (
    "id",
    "title",
    "director",
    "year",
    "item_type",
    "listing_status",
    "stock_status",
    "tc_section",
    "tc_condition",
    "sale_price",
    "image_path",
    "updated_at",
)

COLUMN_TYPES = {
    "title": "TEXT",
    "original_title": "TEXT",
    "item_type": "TEXT",
    "director": "TEXT",
    "writers": "TEXT",
    "actors": "TEXT",
    "year": "TEXT",
    "rated": "TEXT",
    "released": "TEXT",
    "runtime": "TEXT",
    "genres": "TEXT",
    "country": "TEXT",
    "languages": "TEXT",
    "plot": "TEXT",
    "awards": "TEXT",
    "production": "TEXT",
    "imdb_url": "TEXT",
    "imdb_rating": "TEXT",
    "imdb_votes": "TEXT",
    "box_office": "TEXT",
    "sale_price": "REAL",
    "listing_status": "TEXT",
    "stock_status": "TEXT",
    "tc_section": "TEXT",
    "tc_condition": "TEXT",
    "condition_comments": "TEXT",
    "notes": "TEXT",
    "image_path": "TEXT",
    "updated_at": "TIMESTAMP",
}


def table_columns(con, table_name: str = TABLE_NAME) -> set[str]:
    rows = con.execute(f"PRAGMA table_info('{table_name}')").fetchall()
    return {str(row[1]) for row in rows}


def ensure_table(con) -> None:
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id TEXT PRIMARY KEY,
            title TEXT,
            original_title TEXT,
            item_type TEXT,
            director TEXT,
            writers TEXT,
            actors TEXT,
            year TEXT,
            rated TEXT,
            released TEXT,
            runtime TEXT,
            genres TEXT,
            country TEXT,
            languages TEXT,
            plot TEXT,
            awards TEXT,
            production TEXT,
            imdb_url TEXT,
            imdb_rating TEXT,
            imdb_votes TEXT,
            box_office TEXT,
            sale_price REAL,
            listing_status TEXT,
            stock_status TEXT,
            tc_section TEXT,
            tc_condition TEXT,
            condition_comments TEXT,
            notes TEXT,
            image_path TEXT,
            updated_at TIMESTAMP
        )
        """)
    existing = table_columns(con)
    for column_name, column_type in COLUMN_TYPES.items():
        if column_name not in existing:
            con.execute(f"ALTER TABLE {TABLE_NAME} ADD COLUMN {column_name} {column_type}")
    con.execute(f"ALTER TABLE {TABLE_NAME} ALTER COLUMN tc_section TYPE TEXT")


def _is_blank(value: Any) -> bool:
    return not str(value or "").strip()


_BACKSLASH = chr(92)


def _is_windows_absolute_path(path_text: str) -> bool:
    win_path = PureWindowsPath(path_text)
    return bool(win_path.drive and win_path.root)


def _stored_image_path(path: str | Path) -> str:
    candidate = Path(path).expanduser()
    if not candidate.is_absolute():
        candidate = PROJECT_ROOT / candidate
    resolved = candidate.resolve()
    try:
        return resolved.relative_to(PROJECT_ROOT).as_posix()
    except ValueError:
        return resolved.as_posix()


def _basename_from_path(path_text: str) -> str | None:
    raw = str(path_text or "").strip()
    if not raw:
        return None
    win_name = PureWindowsPath(raw).name
    if win_name:
        return win_name
    normalized = raw.replace(_BACKSLASH, "/")
    name = normalized.rsplit("/", 1)[-1].strip()
    return name or None


def _copy_inside_project(path: Path) -> Path:
    resolved = path.expanduser().resolve()
    try:
        resolved.relative_to(PROJECT_ROOT)
        return resolved
    except ValueError:
        destination = DEFAULT_COVERS_DIR / resolved.name
        destination.parent.mkdir(parents=True, exist_ok=True)
        if destination.resolve() != resolved:
            shutil.copy2(resolved, destination)
        return destination.resolve()


def normalize_image_path_value(value: Any) -> str | None:
    current = str(value or "").strip()
    if not current:
        return None
    if current.startswith(("http://", "https://")):
        return current

    is_windows_absolute = _is_windows_absolute_path(current)
    name = _basename_from_path(current)

    if not (is_windows_absolute and not Path(current).is_absolute()):
        candidate = Path(current.replace(_BACKSLASH, "/")).expanduser()
        if not candidate.is_absolute():
            candidate = PROJECT_ROOT / candidate
        resolved = candidate.resolve()
        if resolved.exists() and resolved.is_file():
            return _stored_image_path(_copy_inside_project(resolved))
        try:
            return resolved.relative_to(PROJECT_ROOT).as_posix()
        except ValueError:
            pass

    if (Path(current).is_absolute() or is_windows_absolute) and name:
        return _stored_image_path(DEFAULT_COVERS_DIR / name)

    return current.replace(_BACKSLASH, "/")


def normalize_image_paths(con) -> int:
    ensure_table(con)
    rows = con.execute(
        f"""
        SELECT id, image_path
        FROM {TABLE_NAME}
        WHERE image_path IS NOT NULL
          AND TRIM(image_path) <> ''
        """
    ).fetchall()

    updated = 0
    for item_id, raw_path in rows:
        normalized = normalize_image_path_value(raw_path)
        current = str(raw_path or "").strip()
        if normalized and normalized != current:
            con.execute(
                f"""
                UPDATE {TABLE_NAME}
                SET image_path = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (normalized, item_id),
            )
            updated += 1
    return updated


def _relation_names(con) -> set[str]:
    return {str(row[0]) for row in con.execute("PRAGMA show_tables").fetchall()}


def backfill_omdb_structured_fields(con) -> int:
    ensure_table(con)
    if "movies" not in _relation_names(con):
        return 0

    rows = con.execute(
        f"""
        SELECT item.id, item.genres, item.country, item.languages, item.item_type,
               movie.omdb_genre, movie.omdb_country, movie.omdb_language, movie.omdb_type
        FROM {TABLE_NAME} AS item
        LEFT JOIN movies AS movie ON movie.id = item.id
        ORDER BY LOWER(item.id), item.id
        """
    ).fetchall()

    updated = 0
    for (
        item_id,
        genres,
        country,
        languages,
        item_type,
        omdb_genre,
        omdb_country,
        omdb_language,
        omdb_type,
    ) in rows:
        candidates = {
            "genres": (genres, omdb_genre),
            "country": (country, omdb_country),
            "languages": (languages, omdb_language),
            "item_type": (item_type, omdb_type),
        }
        changes = {
            field: str(source).strip()
            for field, (current, source) in candidates.items()
            if _is_blank(current) and not _is_blank(source)
        }
        if changes:
            assignments = ", ".join(f"{field} = ?" for field in changes)
            con.execute(
                f"""
                UPDATE {TABLE_NAME}
                SET {assignments}, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                [*changes.values(), item_id],
            )
            updated += 1
    return updated


def normalize_translated_fields(con) -> int:
    ensure_table(con)
    rows = con.execute(
        f"""
        SELECT id, genres, country, languages, item_type
        FROM {TABLE_NAME}
        ORDER BY LOWER(id), id
        """
    ).fetchall()

    updated = 0
    for item_id, genres, country, languages, item_type in rows:
        translated = {
            "genres": translate_omdb_field(genres, "genres"),
            "country": translate_omdb_field(country, "country"),
            "languages": translate_omdb_field(languages, "languages"),
            "item_type": translate_omdb_field(item_type, "item_type"),
        }
        current = {
            "genres": genres,
            "country": country,
            "languages": languages,
            "item_type": item_type,
        }
        changes = {
            field: value
            for field, value in translated.items()
            if value is not None and value != current.get(field)
        }
        if changes:
            assignments = ", ".join(f"{field} = ?" for field in changes)
            con.execute(
                f"""
                UPDATE {TABLE_NAME}
                SET {assignments}, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                [*changes.values(), item_id],
            )
            updated += 1
    return updated


def insert_missing_from_movies(con) -> int:
    ensure_table(con)
    before = int(con.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0])
    con.execute(f"""
        INSERT INTO {TABLE_NAME} (
            id, title, original_title, item_type, director, writers, actors, year,
            rated, released, runtime, genres, country, languages, plot,
            awards, production, imdb_url, imdb_rating, imdb_votes, box_office,
            listing_status, stock_status, image_path, updated_at
        )
        SELECT
            movie.id,
            COALESCE(
                NULLIF(TRIM(movie.imdb_title_es), ''),
                NULLIF(TRIM(movie.manual_title), ''),
                NULLIF(TRIM(movie.extraction_title), ''),
                NULLIF(TRIM(movie.imdb_title_original), ''),
                NULLIF(TRIM(movie.omdb_title), '')
            ),
            COALESCE(
                NULLIF(TRIM(movie.imdb_title_original), ''),
                NULLIF(TRIM(movie.omdb_title), '')
            ),
            NULLIF(TRIM(movie.omdb_type), ''),
            NULLIF(TRIM(movie.omdb_director), ''),
            NULLIF(TRIM(movie.omdb_writer), ''),
            NULLIF(TRIM(movie.omdb_actors), ''),
            NULLIF(TRIM(movie.omdb_year), ''),
            NULLIF(TRIM(movie.omdb_rated), ''),
            NULLIF(TRIM(movie.omdb_released), ''),
            NULLIF(TRIM(movie.omdb_runtime), ''),
            NULLIF(TRIM(movie.omdb_genre), ''),
            NULLIF(TRIM(movie.omdb_country), ''),
            NULLIF(TRIM(movie.omdb_language), ''),
            COALESCE(
                NULLIF(TRIM(movie.omdb_plot_es), ''),
                NULLIF(TRIM(movie.omdb_plot_en), '')
            ),
            NULLIF(TRIM(movie.omdb_awards), ''),
            NULLIF(TRIM(movie.omdb_production), ''),
            NULLIF(TRIM(movie.imdb_url), ''),
            NULLIF(TRIM(movie.omdb_imdbrating), ''),
            NULLIF(TRIM(movie.omdb_imdbvotes), ''),
            NULLIF(TRIM(movie.omdb_boxoffice), ''),
            'ALTA',
            'En stock',
            NULLIF(TRIM(movie.image_path), ''),
            CURRENT_TIMESTAMP
        FROM movies AS movie
        LEFT JOIN {TABLE_NAME} AS item ON item.id = movie.id
        WHERE item.id IS NULL
        """)
    after = int(con.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0])
    created = after - before
    if created:
        normalize_translated_fields(con)
    return created


def list_records(con) -> list[dict[str, Any]]:
    cursor = con.execute(f"""
        SELECT {", ".join(SUMMARY_COLUMNS)}
        FROM {TABLE_NAME}
        ORDER BY LOWER(id), id
        """)
    columns = [str(description[0]) for description in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def get_record(con, item_id: str) -> dict[str, Any] | None:
    cursor = con.execute(
        f"SELECT {', '.join(COLUMNS)} FROM {TABLE_NAME} WHERE id = ?",
        (item_id,),
    )
    row = cursor.fetchone()
    if row is None:
        return None
    columns = [str(description[0]) for description in cursor.description]
    return dict(zip(columns, row))


def exists(con, item_id: str) -> bool:
    row = con.execute(
        f"SELECT 1 FROM {TABLE_NAME} WHERE id = ? LIMIT 1",
        (item_id,),
    ).fetchone()
    return row is not None


def update_fields(
    con, item_id: str, fields: dict[str, Any], *, updated_at: datetime | None = None
) -> None:
    if not fields:
        return
    assignments = ", ".join(f"{column} = ?" for column in fields)
    con.execute(
        f"""
        UPDATE {TABLE_NAME}
        SET {assignments}, updated_at = ?
        WHERE id = ?
        """,
        [*fields.values(), updated_at or datetime.now(), item_id],
    )
