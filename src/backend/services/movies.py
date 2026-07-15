import json
import re
import shutil
from datetime import datetime
from pathlib import Path, PureWindowsPath
from typing import Any

from ..config import DEFAULT_COVERS_DIR, PROJECT_ROOT
from ..multi_value import join_values, split_values
from ..database import get_connection
from ..omdb_dictionaries import translate_omdb_field, translate_omdb_fields
from ..normalizers import (
    canonical_imdb_url,
    extract_imdb_id,
    parse_json_list,
)

WORKFLOW_STAGE_ORDER = {
    "extraction": 1,
    "imdb": 2,
    "title_es": 3,
    "omdb": 4,
    "translation": 5,
}

_EFFECTIVE_TITLE_SQL = "COALESCE(NULLIF(TRIM(manual_title), ''), NULLIF(TRIM(extraction_title), ''))"
_EFFECTIVE_TEAM_SQL = (
    "CASE "
    "WHEN manual_team_json IS NOT NULL "
    "AND TRIM(CAST(manual_team_json AS VARCHAR)) NOT IN ('', '[]', 'null', 'NULL') "
    "THEN TRIM(CAST(manual_team_json AS VARCHAR)) "
    "ELSE TRIM(COALESCE(CAST(extraction_team_json AS VARCHAR), '')) "
    "END"
)
_MANUAL_TITLE_PRESENT_SQL = "manual_title IS NOT NULL AND TRIM(manual_title) <> ''"
_MANUAL_TEAM_PRESENT_SQL = (
    "manual_team_json IS NOT NULL "
    "AND TRIM(CAST(manual_team_json AS VARCHAR)) NOT IN ('', '[]', 'null', 'NULL')"
)
_MANUAL_OVERRIDE_SQL = f"({_MANUAL_TITLE_PRESENT_SQL} OR {_MANUAL_TEAM_PRESENT_SQL})"
_MISSING_EXTRACTION_SQL = (
    f"(NOT {_MANUAL_OVERRIDE_SQL} "
    f"AND ({_EFFECTIVE_TITLE_SQL} IS NULL OR {_EFFECTIVE_TEAM_SQL} IN ('', '[]', 'null', 'NULL')))"
)
_TITLE_PARTS_SQL = (
    f"(1 + LENGTH(TRIM({_EFFECTIVE_TITLE_SQL})) - LENGTH(REPLACE(TRIM({_EFFECTIVE_TITLE_SQL}), ';', '')))"
)
_IMDB_URL_PARTS_SQL = "(1 + LENGTH(TRIM(imdb_url)) - LENGTH(REPLACE(TRIM(imdb_url), ';', '')))"
_IMDB_ID_PARTS_SQL = "(1 + LENGTH(TRIM(imdb_id)) - LENGTH(REPLACE(TRIM(imdb_id), ';', '')))"
_IMDB_TITLE_ES_PARTS_SQL = (
    "(1 + LENGTH(TRIM(imdb_title_es)) - LENGTH(REPLACE(TRIM(imdb_title_es), ';', '')))"
)
_OMDB_TITLE_PARTS_SQL = "(1 + LENGTH(TRIM(omdb_title)) - LENGTH(REPLACE(TRIM(omdb_title), ';', '')))"
_PLOT_EN_PARTS_SQL = (
    "(1 + ((LENGTH(TRIM(omdb_plot_en)) - LENGTH(REPLACE(TRIM(omdb_plot_en), ';\n', ''))) / 2))"
)
_PLOT_ES_PARTS_SQL = (
    "(1 + ((LENGTH(TRIM(omdb_plot_es)) - LENGTH(REPLACE(TRIM(omdb_plot_es), ';\n', ''))) / 2))"
)
IMAGE_EXTENSIONS = (".jpg", ".jpeg", ".png", ".heic", ".webp")
VALID_MOVIE_ID_PATTERN = re.compile(r"^P\d{4}$")
LEGACY_LEADING_ZERO_PATTERN = re.compile(r"^P0\d{4}$")

CORE_TABLE = "movies_core"
EXTRACTION_TABLE = "movie_extraction"
IMDB_TABLE = "movie_imdb"
OMDB_TABLE = "movie_omdb"
WORKFLOW_TABLE = "movie_workflow"
LEGACY_TABLE = "movies_legacy"
MOVIES_VIEW = "movies"

CORE_COLUMNS = {
    "image_path",
    "image_filename",
    "created_at",
    "updated_at",
}
EXTRACTION_COLUMNS = {
    "extraction_title",
    "extraction_team_json",
    "extraction_title_raw",
    "extraction_team_raw",
    "manual_title",
    "manual_team_json",
}
IMDB_COLUMNS = {
    "imdb_query",
    "imdb_url",
    "imdb_id",
    "imdb_status",
    "imdb_last_error",
    "imdb_title_es",
    "imdb_title_es_status",
    "imdb_title_es_last_error",
    "imdb_title_original",
    "imdb_title_original_status",
    "imdb_title_original_last_error",
}
OMDB_COLUMNS = {
    "omdb_raw_json",
    "omdb_status",
    "omdb_last_error",
    "omdb_title",
    "omdb_year",
    "omdb_rated",
    "omdb_released",
    "omdb_runtime",
    "omdb_genre",
    "omdb_director",
    "omdb_writer",
    "omdb_actors",
    "omdb_plot_en",
    "omdb_plot_es",
    "omdb_language",
    "omdb_country",
    "omdb_awards",
    "omdb_poster",
    "omdb_imdbrating",
    "omdb_imdbvotes",
    "omdb_type",
    "omdb_dvd",
    "omdb_boxoffice",
    "omdb_production",
    "translation_status",
    "translation_last_error",
}
WORKFLOW_COLUMNS = {
    "workflow_status",
    "workflow_current_node",
    "workflow_needs_review",
    "workflow_review_reason",
    "workflow_attempt",
    "workflow_last_action",
    "workflow_last_error",
    "workflow_history_json",
}

COLUMN_TABLE_MAP: dict[str, str] = {}
for _column in CORE_COLUMNS:
    COLUMN_TABLE_MAP[_column] = CORE_TABLE
for _column in EXTRACTION_COLUMNS:
    COLUMN_TABLE_MAP[_column] = EXTRACTION_TABLE
for _column in IMDB_COLUMNS:
    COLUMN_TABLE_MAP[_column] = IMDB_TABLE
for _column in OMDB_COLUMNS:
    COLUMN_TABLE_MAP[_column] = OMDB_TABLE
for _column in WORKFLOW_COLUMNS:
    COLUMN_TABLE_MAP[_column] = WORKFLOW_TABLE


def _effective_title_from_dict(movie: dict[str, Any]) -> str:
    for key in ("manual_title", "extraction_title"):
        value = str(movie.get(key) or "").strip()
        if value:
            return value
    return ""


def _effective_team_from_dict(movie: dict[str, Any]) -> list[str]:
    manual_team = parse_json_list(movie.get("manual_team_json"))
    if not manual_team:
        manual_team = parse_json_list(movie.get("manual_team"))
    if manual_team:
        return manual_team

    extraction_team = parse_json_list(movie.get("extraction_team_json"))
    if not extraction_team:
        extraction_team = parse_json_list(movie.get("extraction_team"))
    return extraction_team


def _has_manual_override_from_dict(movie: dict[str, Any]) -> bool:
    manual_title = str(movie.get("manual_title") or "").strip()
    if manual_title:
        return True

    manual_team = parse_json_list(movie.get("manual_team_json"))
    if not manual_team:
        manual_team = parse_json_list(movie.get("manual_team"))
    return bool(manual_team)


def _has_complete_multi_value(base: str, candidate: str) -> bool:
    base_parts = split_values(base)
    if not base_parts:
        return False

    candidate_parts = split_values(candidate)
    if not candidate_parts:
        return False

    if len(base_parts) <= 1:
        return True
    return len(candidate_parts) == len(base_parts)


def _has_complete_plot_value(plot_en: str, plot_es: str) -> bool:
    base_parts = _split_plot_source_parts(plot_en)
    if not base_parts:
        return False

    if len(base_parts) <= 1:
        return bool(str(plot_es or "").strip())

    candidate_parts = _split_plot_candidate_parts(plot_es, expected_count=len(base_parts))
    return len(candidate_parts) == len(base_parts)


def _split_plot_source_parts(plot_text: str | None) -> list[str]:
    text = str(plot_text or "").strip()
    if not text:
        return []

    if ";\n" in text:
        parts = split_values(text, separator=";\n")
        if parts:
            return parts

    return [text]


def _split_on_blank_lines(plot_text: str | None) -> list[str]:
    text = str(plot_text or "").strip()
    if not text:
        return []
    return [part.strip() for part in re.split(r"\n\s*\n+", text) if part.strip()]


def _split_plot_candidate_parts(plot_text: str | None, *, expected_count: int) -> list[str]:
    text = str(plot_text or "").strip()
    if not text:
        return []

    parts = split_values(text, separator=";\n")
    if expected_count <= 1 or len(parts) == expected_count:
        return parts

    if ";" in text:
        semicolon_parts = split_values(text, separator=";")
        if len(semicolon_parts) == expected_count:
            return semicolon_parts

    blank_parts = _split_on_blank_lines(text)
    if len(blank_parts) == expected_count:
        return blank_parts

    return parts


def _normalize_plot_es_text(plot_en: str | None, plot_es: str | None) -> str | None:
    clean_plot_es = str(plot_es or "").strip()
    if not clean_plot_es:
        return None if plot_es is None else ""

    source_parts = _split_plot_source_parts(plot_en)
    if len(source_parts) <= 1:
        return clean_plot_es

    candidate_parts = _split_plot_candidate_parts(clean_plot_es, expected_count=len(source_parts))
    if len(candidate_parts) != len(source_parts):
        return clean_plot_es

    return join_values(candidate_parts, separator=";\n", keep_empty=True)


def is_plot_translation_complete(plot_en: str | None, plot_es: str | None) -> bool:
    return _has_complete_plot_value(str(plot_en or ""), str(plot_es or ""))


def _derive_pipeline_stage_from_dict(movie: dict[str, Any]) -> str:
    if bool(movie.get("workflow_needs_review")):
        return "review"

    workflow_status = str(movie.get("workflow_status") or "").lower()
    workflow_node = str(movie.get("workflow_current_node") or "").strip()
    if workflow_status == "running":
        return f"running:{workflow_node}" if workflow_node else "running"

    effective_title = _effective_title_from_dict(movie)
    effective_team = _effective_team_from_dict(movie)
    has_manual_override = _has_manual_override_from_dict(movie)
    if not has_manual_override and (not effective_title or not effective_team):
        return "extraction"

    imdb_url = str(movie.get("imdb_url") or "").strip()
    if not _has_complete_multi_value(effective_title, imdb_url):
        return "imdb"

    imdb_title_es = str(movie.get("imdb_title_es") or "").strip()
    if not _has_complete_multi_value(imdb_url, imdb_title_es):
        return "title_es"

    omdb_status = str(movie.get("omdb_status") or "").lower()
    imdb_id = str(movie.get("imdb_id") or "").strip()
    omdb_title = str(movie.get("omdb_title") or "").strip()
    if omdb_status != "fetched" or (imdb_id and not _has_complete_multi_value(imdb_id, omdb_title)):
        return "omdb"

    omdb_plot_en = str(movie.get("omdb_plot_en") or "").strip()
    omdb_plot_es = str(movie.get("omdb_plot_es") or "").strip()
    if omdb_plot_en and not _has_complete_plot_value(omdb_plot_en, omdb_plot_es):
        return "translation"

    return "done"


def ensure_schema(con) -> None:
    _migrate_legacy_movies_table_if_needed(con)
    _create_normalized_tables(con)
    _migrate_from_legacy_table(con)
    _ensure_all_companion_rows(con)
    _normalize_stored_image_paths(con)
    _recreate_movies_view(con)


def init_table() -> None:
    con = get_connection()
    try:
        ensure_schema(con)
    finally:
        con.close()



def _relation_type(con, relation_name: str) -> str | None:
    row = con.execute(
        """
        SELECT table_type
        FROM information_schema.tables
        WHERE table_schema = current_schema()
          AND table_name = ?
        """,
        (relation_name,),
    ).fetchone()
    return str(row[0]).upper() if row else None


def _table_columns(con, table_name: str) -> set[str]:
    try:
        rows = con.execute(f"PRAGMA table_info({table_name})").fetchall()
    except Exception:
        return set()
    return {str(row[1]) for row in rows}


def _legacy_value_expr(legacy_columns: set[str], column_name: str, default_sql: str) -> str:
    return f"l.{column_name}" if column_name in legacy_columns else default_sql


def _migrate_legacy_movies_table_if_needed(con) -> None:
    relation = _relation_type(con, MOVIES_VIEW)
    if relation != "BASE TABLE":
        return
    if _relation_type(con, LEGACY_TABLE) is None:
        con.execute(f"ALTER TABLE {MOVIES_VIEW} RENAME TO {LEGACY_TABLE}")


def _create_normalized_tables(con) -> None:
    con.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {CORE_TABLE} (
            id TEXT PRIMARY KEY,
            image_path TEXT NOT NULL,
            image_filename TEXT,
            created_at TIMESTAMP DEFAULT now(),
            updated_at TIMESTAMP DEFAULT now()
        )
        """
    )
    con.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {EXTRACTION_TABLE} (
            id TEXT PRIMARY KEY,
            extraction_title TEXT,
            extraction_team_json JSON,
            extraction_title_raw TEXT,
            extraction_team_raw TEXT,
            manual_title TEXT,
            manual_team_json JSON
        )
        """
    )
    con.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {IMDB_TABLE} (
            id TEXT PRIMARY KEY,
            imdb_query TEXT,
            imdb_url TEXT,
            imdb_id TEXT,
            imdb_status TEXT DEFAULT 'pending',
            imdb_last_error TEXT,
            imdb_title_es TEXT,
            imdb_title_es_status TEXT DEFAULT 'pending',
            imdb_title_es_last_error TEXT,
            imdb_title_original TEXT,
            imdb_title_original_status TEXT DEFAULT 'pending',
            imdb_title_original_last_error TEXT
        )
        """
    )
    con.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {OMDB_TABLE} (
            id TEXT PRIMARY KEY,
            omdb_raw_json JSON,
            omdb_status TEXT DEFAULT 'pending',
            omdb_last_error TEXT,
            omdb_title TEXT,
            omdb_year TEXT,
            omdb_rated TEXT,
            omdb_released TEXT,
            omdb_runtime TEXT,
            omdb_genre TEXT,
            omdb_director TEXT,
            omdb_writer TEXT,
            omdb_actors TEXT,
            omdb_plot_en TEXT,
            omdb_plot_es TEXT,
            omdb_language TEXT,
            omdb_country TEXT,
            omdb_awards TEXT,
            omdb_poster TEXT,
            omdb_imdbrating TEXT,
            omdb_imdbvotes TEXT,
            omdb_type TEXT,
            omdb_dvd TEXT,
            omdb_boxoffice TEXT,
            omdb_production TEXT,
            translation_status TEXT DEFAULT 'pending',
            translation_last_error TEXT
        )
        """
    )
    con.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {WORKFLOW_TABLE} (
            id TEXT PRIMARY KEY,
            workflow_status TEXT DEFAULT 'pending',
            workflow_current_node TEXT,
            workflow_needs_review BOOLEAN DEFAULT FALSE,
            workflow_review_reason TEXT,
            workflow_attempt INTEGER DEFAULT 0,
            workflow_last_action TEXT,
            workflow_last_error TEXT,
            workflow_history_json JSON
        )
        """
    )


def _migrate_from_legacy_table(con) -> None:
    if _relation_type(con, LEGACY_TABLE) != "BASE TABLE":
        return

    legacy_columns = _table_columns(con, LEGACY_TABLE)
    if "id" not in legacy_columns:
        return

    con.execute(
        f"""
        INSERT INTO {CORE_TABLE} (id, image_path, image_filename, created_at, updated_at)
        SELECT
            {_legacy_value_expr(legacy_columns, "id", "NULL")},
            {_legacy_value_expr(legacy_columns, "image_path", "''")},
            {_legacy_value_expr(legacy_columns, "image_filename", "NULL")},
            {_legacy_value_expr(legacy_columns, "created_at", "now()")},
            {_legacy_value_expr(legacy_columns, "updated_at", "now()")}
        FROM {LEGACY_TABLE} l
        LEFT JOIN {CORE_TABLE} c ON c.id = l.id
        WHERE c.id IS NULL
          AND l.id IS NOT NULL
          AND TRIM(CAST(l.id AS VARCHAR)) <> ''
        """
    )
    con.execute(
        f"""
        INSERT INTO {EXTRACTION_TABLE} (
            id,
            extraction_title,
            extraction_team_json,
            extraction_title_raw,
            extraction_team_raw,
            manual_title,
            manual_team_json
        )
        SELECT
            {_legacy_value_expr(legacy_columns, "id", "NULL")},
            {_legacy_value_expr(legacy_columns, "extraction_title", "NULL")},
            {_legacy_value_expr(legacy_columns, "extraction_team_json", "NULL")},
            {_legacy_value_expr(legacy_columns, "extraction_title_raw", "NULL")},
            {_legacy_value_expr(legacy_columns, "extraction_team_raw", "NULL")},
            {_legacy_value_expr(legacy_columns, "manual_title", "NULL")},
            {_legacy_value_expr(legacy_columns, "manual_team_json", "NULL")}
        FROM {LEGACY_TABLE} l
        LEFT JOIN {EXTRACTION_TABLE} t ON t.id = l.id
        WHERE t.id IS NULL
          AND l.id IS NOT NULL
          AND TRIM(CAST(l.id AS VARCHAR)) <> ''
        """
    )
    con.execute(
        f"""
        INSERT INTO {IMDB_TABLE} (
            id,
            imdb_query,
            imdb_url,
            imdb_id,
            imdb_status,
            imdb_last_error,
            imdb_title_es,
            imdb_title_es_status,
            imdb_title_es_last_error,
            imdb_title_original,
            imdb_title_original_status,
            imdb_title_original_last_error
        )
        SELECT
            {_legacy_value_expr(legacy_columns, "id", "NULL")},
            {_legacy_value_expr(legacy_columns, "imdb_query", "NULL")},
            {_legacy_value_expr(legacy_columns, "imdb_url", "NULL")},
            {_legacy_value_expr(legacy_columns, "imdb_id", "NULL")},
            {_legacy_value_expr(legacy_columns, "imdb_status", "'pending'")},
            {_legacy_value_expr(legacy_columns, "imdb_last_error", "NULL")},
            {_legacy_value_expr(legacy_columns, "imdb_title_es", "NULL")},
            {_legacy_value_expr(legacy_columns, "imdb_title_es_status", "'pending'")},
            {_legacy_value_expr(legacy_columns, "imdb_title_es_last_error", "NULL")},
            {_legacy_value_expr(legacy_columns, "imdb_title_original", "NULL")},
            {_legacy_value_expr(legacy_columns, "imdb_title_original_status", "'pending'")},
            {_legacy_value_expr(legacy_columns, "imdb_title_original_last_error", "NULL")}
        FROM {LEGACY_TABLE} l
        LEFT JOIN {IMDB_TABLE} t ON t.id = l.id
        WHERE t.id IS NULL
          AND l.id IS NOT NULL
          AND TRIM(CAST(l.id AS VARCHAR)) <> ''
        """
    )
    con.execute(
        f"""
        INSERT INTO {OMDB_TABLE} (
            id,
            omdb_raw_json,
            omdb_status,
            omdb_last_error,
            omdb_title,
            omdb_year,
            omdb_rated,
            omdb_released,
            omdb_runtime,
            omdb_genre,
            omdb_director,
            omdb_writer,
            omdb_actors,
            omdb_plot_en,
            omdb_plot_es,
            omdb_language,
            omdb_country,
            omdb_awards,
            omdb_poster,
            omdb_imdbrating,
            omdb_imdbvotes,
            omdb_type,
            omdb_dvd,
            omdb_boxoffice,
            omdb_production,
            translation_status,
            translation_last_error
        )
        SELECT
            {_legacy_value_expr(legacy_columns, "id", "NULL")},
            {_legacy_value_expr(legacy_columns, "omdb_raw_json", "NULL")},
            {_legacy_value_expr(legacy_columns, "omdb_status", "'pending'")},
            {_legacy_value_expr(legacy_columns, "omdb_last_error", "NULL")},
            {_legacy_value_expr(legacy_columns, "omdb_title", "NULL")},
            {_legacy_value_expr(legacy_columns, "omdb_year", "NULL")},
            {_legacy_value_expr(legacy_columns, "omdb_rated", "NULL")},
            {_legacy_value_expr(legacy_columns, "omdb_released", "NULL")},
            {_legacy_value_expr(legacy_columns, "omdb_runtime", "NULL")},
            {_legacy_value_expr(legacy_columns, "omdb_genre", "NULL")},
            {_legacy_value_expr(legacy_columns, "omdb_director", "NULL")},
            {_legacy_value_expr(legacy_columns, "omdb_writer", "NULL")},
            {_legacy_value_expr(legacy_columns, "omdb_actors", "NULL")},
            {_legacy_value_expr(legacy_columns, "omdb_plot_en", "NULL")},
            {_legacy_value_expr(legacy_columns, "omdb_plot_es", "NULL")},
            {_legacy_value_expr(legacy_columns, "omdb_language", "NULL")},
            {_legacy_value_expr(legacy_columns, "omdb_country", "NULL")},
            {_legacy_value_expr(legacy_columns, "omdb_awards", "NULL")},
            {_legacy_value_expr(legacy_columns, "omdb_poster", "NULL")},
            {_legacy_value_expr(legacy_columns, "omdb_imdbrating", "NULL")},
            {_legacy_value_expr(legacy_columns, "omdb_imdbvotes", "NULL")},
            {_legacy_value_expr(legacy_columns, "omdb_type", "NULL")},
            {_legacy_value_expr(legacy_columns, "omdb_dvd", "NULL")},
            {_legacy_value_expr(legacy_columns, "omdb_boxoffice", "NULL")},
            {_legacy_value_expr(legacy_columns, "omdb_production", "NULL")},
            {_legacy_value_expr(legacy_columns, "translation_status", "'pending'")},
            {_legacy_value_expr(legacy_columns, "translation_last_error", "NULL")}
        FROM {LEGACY_TABLE} l
        LEFT JOIN {OMDB_TABLE} t ON t.id = l.id
        WHERE t.id IS NULL
          AND l.id IS NOT NULL
          AND TRIM(CAST(l.id AS VARCHAR)) <> ''
        """
    )
    con.execute(
        f"""
        INSERT INTO {WORKFLOW_TABLE} (
            id,
            workflow_status,
            workflow_current_node,
            workflow_needs_review,
            workflow_review_reason,
            workflow_attempt,
            workflow_last_action,
            workflow_last_error,
            workflow_history_json
        )
        SELECT
            {_legacy_value_expr(legacy_columns, "id", "NULL")},
            {_legacy_value_expr(legacy_columns, "workflow_status", "'pending'")},
            {_legacy_value_expr(legacy_columns, "workflow_current_node", "NULL")},
            {_legacy_value_expr(legacy_columns, "workflow_needs_review", "FALSE")},
            {_legacy_value_expr(legacy_columns, "workflow_review_reason", "NULL")},
            {_legacy_value_expr(legacy_columns, "workflow_attempt", "0")},
            {_legacy_value_expr(legacy_columns, "workflow_last_action", "NULL")},
            {_legacy_value_expr(legacy_columns, "workflow_last_error", "NULL")},
            {_legacy_value_expr(legacy_columns, "workflow_history_json", "NULL")}
        FROM {LEGACY_TABLE} l
        LEFT JOIN {WORKFLOW_TABLE} t ON t.id = l.id
        WHERE t.id IS NULL
          AND l.id IS NOT NULL
          AND TRIM(CAST(l.id AS VARCHAR)) <> ''
        """
    )


def _ensure_companion_rows_for_movie(con, movie_id: str) -> None:
    row = con.execute(
        f"SELECT 1 FROM {CORE_TABLE} WHERE id = ?",
        (movie_id,),
    ).fetchone()
    if row is None:
        return

    con.execute(
        f"""
        INSERT INTO {EXTRACTION_TABLE} (id)
        SELECT ?
        WHERE NOT EXISTS (SELECT 1 FROM {EXTRACTION_TABLE} WHERE id = ?)
        """,
        (movie_id, movie_id),
    )
    con.execute(
        f"""
        INSERT INTO {IMDB_TABLE} (id)
        SELECT ?
        WHERE NOT EXISTS (SELECT 1 FROM {IMDB_TABLE} WHERE id = ?)
        """,
        (movie_id, movie_id),
    )
    con.execute(
        f"""
        INSERT INTO {OMDB_TABLE} (id)
        SELECT ?
        WHERE NOT EXISTS (SELECT 1 FROM {OMDB_TABLE} WHERE id = ?)
        """,
        (movie_id, movie_id),
    )
    con.execute(
        f"""
        INSERT INTO {WORKFLOW_TABLE} (id)
        SELECT ?
        WHERE NOT EXISTS (SELECT 1 FROM {WORKFLOW_TABLE} WHERE id = ?)
        """,
        (movie_id, movie_id),
    )


def _ensure_all_companion_rows(con) -> None:
    con.execute(
        f"""
        INSERT INTO {EXTRACTION_TABLE} (id)
        SELECT c.id
        FROM {CORE_TABLE} c
        LEFT JOIN {EXTRACTION_TABLE} t ON t.id = c.id
        WHERE t.id IS NULL
        """
    )
    con.execute(
        f"""
        INSERT INTO {IMDB_TABLE} (id)
        SELECT c.id
        FROM {CORE_TABLE} c
        LEFT JOIN {IMDB_TABLE} t ON t.id = c.id
        WHERE t.id IS NULL
        """
    )
    con.execute(
        f"""
        INSERT INTO {OMDB_TABLE} (id)
        SELECT c.id
        FROM {CORE_TABLE} c
        LEFT JOIN {OMDB_TABLE} t ON t.id = c.id
        WHERE t.id IS NULL
        """
    )
    con.execute(
        f"""
        INSERT INTO {WORKFLOW_TABLE} (id)
        SELECT c.id
        FROM {CORE_TABLE} c
        LEFT JOIN {WORKFLOW_TABLE} t ON t.id = c.id
        WHERE t.id IS NULL
        """
    )


def _recreate_movies_view(con) -> None:
    relation = _relation_type(con, MOVIES_VIEW)
    if relation == "VIEW":
        con.execute(f"DROP VIEW IF EXISTS {MOVIES_VIEW}")
    elif relation == "BASE TABLE":
        if _relation_type(con, LEGACY_TABLE) is None:
            con.execute(f"ALTER TABLE {MOVIES_VIEW} RENAME TO {LEGACY_TABLE}")
        else:
            backup_name = f"{LEGACY_TABLE}_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
            con.execute(f"ALTER TABLE {MOVIES_VIEW} RENAME TO {backup_name}")

    con.execute(
        f"""
        CREATE VIEW {MOVIES_VIEW} AS
        SELECT
            c.id,
            c.image_path,
            c.image_filename,
            e.extraction_title,
            e.extraction_team_json,
            e.extraction_title_raw,
            e.extraction_team_raw,
            e.manual_title,
            e.manual_team_json,
            i.imdb_query,
            i.imdb_url,
            i.imdb_id,
            i.imdb_status,
            i.imdb_last_error,
            i.imdb_title_es,
            i.imdb_title_es_status,
            i.imdb_title_es_last_error,
            i.imdb_title_original,
            i.imdb_title_original_status,
            i.imdb_title_original_last_error,
            o.omdb_raw_json,
            o.omdb_status,
            o.omdb_last_error,
            o.omdb_title,
            o.omdb_year,
            o.omdb_rated,
            o.omdb_released,
            o.omdb_runtime,
            o.omdb_genre,
            o.omdb_director,
            o.omdb_writer,
            o.omdb_actors,
            o.omdb_plot_en,
            o.omdb_plot_es,
            o.omdb_language,
            o.omdb_country,
            o.omdb_awards,
            o.omdb_poster,
            o.omdb_imdbrating,
            o.omdb_imdbvotes,
            o.omdb_type,
            o.omdb_dvd,
            o.omdb_boxoffice,
            o.omdb_production,
            o.translation_status,
            o.translation_last_error,
            w.workflow_status,
            w.workflow_current_node,
            w.workflow_needs_review,
            w.workflow_review_reason,
            w.workflow_attempt,
            w.workflow_last_action,
            w.workflow_last_error,
            w.workflow_history_json,
            c.created_at,
            c.updated_at
        FROM {CORE_TABLE} c
        LEFT JOIN {EXTRACTION_TABLE} e ON e.id = c.id
        LEFT JOIN {IMDB_TABLE} i ON i.id = c.id
        LEFT JOIN {OMDB_TABLE} o ON o.id = c.id
        LEFT JOIN {WORKFLOW_TABLE} w ON w.id = c.id
        """
    )


def _touch_movie(con, movie_id: str) -> None:
    con.execute(
        f"UPDATE {CORE_TABLE} SET updated_at = now() WHERE id = ?",
        (movie_id,),
    )



def _load_json(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return value
    return value



def _serialize_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False)



def _append_workflow_history(
    movie_id: str,
    *,
    event_type: str,
    node: str | None,
    message: str | None,
    payload: dict[str, Any] | None = None,
) -> None:
    con = get_connection()
    _ensure_companion_rows_for_movie(con, movie_id)
    row = con.execute(
        f"SELECT workflow_history_json FROM {WORKFLOW_TABLE} WHERE id = ?",
        (movie_id,),
    ).fetchone()

    history_raw = row[0] if row else None
    history = _load_json(history_raw)
    if not isinstance(history, list):
        history = []

    event = {
        "ts": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "type": event_type,
        "node": node,
        "message": message,
        "payload": payload or {},
    }
    history.append(event)
    history = history[-100:]

    con.execute(
        f"""
        UPDATE {WORKFLOW_TABLE}
        SET workflow_history_json = ?
        WHERE id = ?
        """,
        (_serialize_json(history), movie_id),
    )
    _touch_movie(con, movie_id)
    con.close()



def _update_workflow_fields(movie_id: str, fields: dict[str, Any]) -> None:
    clean_fields = {k: v for k, v in fields.items() if k in COLUMN_TABLE_MAP}
    if not clean_fields:
        return

    grouped: dict[str, dict[str, Any]] = {}
    for key, value in clean_fields.items():
        table_name = COLUMN_TABLE_MAP[key]
        grouped.setdefault(table_name, {})[key] = value

    con = get_connection()
    _ensure_companion_rows_for_movie(con, movie_id)
    for table_name, updates in grouped.items():
        assignments = ", ".join(f"{column} = ?" for column in updates)
        values = list(updates.values())
        con.execute(
            f"UPDATE {table_name} SET {assignments} WHERE id = ?",
            values + [movie_id],
        )

    if "updated_at" not in clean_fields:
        _touch_movie(con, movie_id)
    con.close()



def set_workflow_running(movie_id: str, *, node: str, action: str | None = None) -> None:
    fields: dict[str, Any] = {
        "workflow_status": "running",
        "workflow_current_node": node,
        "workflow_last_error": None,
    }
    if action is not None:
        fields["workflow_last_action"] = action

    _update_workflow_fields(movie_id, fields)
    _append_workflow_history(
        movie_id,
        event_type="running",
        node=node,
        message="Workflow running",
        payload={"action": action} if action else None,
    )



def set_workflow_pending(movie_id: str, *, node: str, reason: str | None = None) -> None:
    _update_workflow_fields(
        movie_id,
        {
            "workflow_status": "pending",
            "workflow_current_node": node,
            "workflow_last_error": None,
        },
    )
    _append_workflow_history(
        movie_id,
        event_type="pending",
        node=node,
        message=reason or "Workflow paused",
    )



def set_workflow_error(movie_id: str, *, node: str, error: str) -> None:
    _update_workflow_fields(
        movie_id,
        {
            "workflow_status": "running",
            "workflow_current_node": node,
            "workflow_last_error": error,
        },
    )
    _append_workflow_history(
        movie_id,
        event_type="error",
        node=node,
        message=error,
    )



def set_workflow_review(
    movie_id: str,
    *,
    node: str,
    reason: str,
    error: str | None = None,
) -> None:
    _update_workflow_fields(
        movie_id,
        {
            "workflow_status": "review",
            "workflow_current_node": node,
            "workflow_needs_review": True,
            "workflow_review_reason": reason,
            "workflow_last_error": error,
        },
    )
    _append_workflow_history(
        movie_id,
        event_type="review",
        node=node,
        message=reason,
        payload={"error": error} if error else None,
    )



def clear_workflow_review(movie_id: str) -> None:
    _update_workflow_fields(
        movie_id,
        {
            "workflow_needs_review": False,
            "workflow_review_reason": None,
        },
    )



def set_workflow_done(movie_id: str, *, node: str, action: str | None = None) -> None:
    fields: dict[str, Any] = {
        "workflow_status": "done",
        "workflow_current_node": node,
        "workflow_needs_review": False,
        "workflow_review_reason": None,
        "workflow_last_error": None,
    }
    if action is not None:
        fields["workflow_last_action"] = action

    _update_workflow_fields(movie_id, fields)
    _append_workflow_history(
        movie_id,
        event_type="done",
        node=node,
        message="Workflow completed",
        payload={"action": action} if action else None,
    )



def recover_stale_running_workflows(*, reason: str = "Recuperado tras reiniciar el backend") -> int:
    con = get_connection()
    rows = con.execute(
        f"""
        SELECT id
        FROM {WORKFLOW_TABLE}
        WHERE workflow_status = 'running'
        """
    ).fetchall()

    stale_ids = [row[0] for row in rows]
    if not stale_ids:
        con.close()
        return 0

    con.execute(
        f"""
        UPDATE {WORKFLOW_TABLE}
        SET
            workflow_status = 'pending',
            workflow_current_node = 'recovered',
            workflow_last_error = CASE
                WHEN workflow_last_error IS NULL OR workflow_last_error = '' THEN ?
                ELSE workflow_last_error
            END
        WHERE workflow_status = 'running'
        """,
        (reason,),
    )
    for movie_id in stale_ids:
        _touch_movie(con, str(movie_id))
    con.close()
    return len(stale_ids)


def increment_workflow_attempt(movie_id: str) -> int:
    con = get_connection()
    row = con.execute(
        f"SELECT COALESCE(workflow_attempt, 0) FROM {WORKFLOW_TABLE} WHERE id = ?",
        (movie_id,),
    ).fetchone()
    current = int(row[0]) if row else 0
    updated = current + 1

    con.execute(
        f"UPDATE {WORKFLOW_TABLE} SET workflow_attempt = ? WHERE id = ?",
        (updated, movie_id),
    )
    _touch_movie(con, movie_id)
    con.close()

    _append_workflow_history(
        movie_id,
        event_type="attempt",
        node="retry",
        message=f"Workflow attempt incremented to {updated}",
    )

    return updated



def reset_workflow_attempt(movie_id: str) -> None:
    _update_workflow_fields(movie_id, {"workflow_attempt": 0})



def reset_from_stage(movie_id: str, stage: str) -> None:
    stage = stage.strip().lower()
    updates: dict[str, Any]

    if stage == "extraction":
        updates = {
            "extraction_title": None,
            "extraction_team_json": None,
            "extraction_title_raw": None,
            "extraction_team_raw": None,
            "manual_title": None,
            "manual_team_json": None,
            "imdb_query": None,
            "imdb_url": None,
            "imdb_id": None,
            "imdb_status": "pending",
            "imdb_last_error": None,
            "imdb_title_es": None,
            "imdb_title_es_status": "pending",
            "imdb_title_es_last_error": None,
            "imdb_title_original": None,
            "imdb_title_original_status": "pending",
            "imdb_title_original_last_error": None,
            "omdb_raw_json": None,
            "omdb_status": "pending",
            "omdb_last_error": None,
            "omdb_title": None,
            "omdb_year": None,
            "omdb_rated": None,
            "omdb_released": None,
            "omdb_runtime": None,
            "omdb_genre": None,
            "omdb_director": None,
            "omdb_writer": None,
            "omdb_actors": None,
            "omdb_plot_en": None,
            "omdb_plot_es": None,
            "omdb_language": None,
            "omdb_country": None,
            "omdb_awards": None,
            "omdb_poster": None,
            "omdb_imdbrating": None,
            "omdb_imdbvotes": None,
            "omdb_type": None,
            "omdb_dvd": None,
            "omdb_boxoffice": None,
            "omdb_production": None,
            "translation_status": "pending",
            "translation_last_error": None,
        }
    elif stage == "imdb":
        updates = {
            "imdb_query": None,
            "imdb_url": None,
            "imdb_id": None,
            "imdb_status": "pending",
            "imdb_last_error": None,
            "imdb_title_es": None,
            "imdb_title_es_status": "pending",
            "imdb_title_es_last_error": None,
            "imdb_title_original": None,
            "imdb_title_original_status": "pending",
            "imdb_title_original_last_error": None,
            "omdb_raw_json": None,
            "omdb_status": "pending",
            "omdb_last_error": None,
            "omdb_title": None,
            "omdb_year": None,
            "omdb_rated": None,
            "omdb_released": None,
            "omdb_runtime": None,
            "omdb_genre": None,
            "omdb_director": None,
            "omdb_writer": None,
            "omdb_actors": None,
            "omdb_plot_en": None,
            "omdb_plot_es": None,
            "omdb_language": None,
            "omdb_country": None,
            "omdb_awards": None,
            "omdb_poster": None,
            "omdb_imdbrating": None,
            "omdb_imdbvotes": None,
            "omdb_type": None,
            "omdb_dvd": None,
            "omdb_boxoffice": None,
            "omdb_production": None,
            "translation_status": "pending",
            "translation_last_error": None,
        }
    elif stage == "title_es":
        updates = {
            "imdb_title_es": None,
            "imdb_title_es_status": "pending",
            "imdb_title_es_last_error": None,
            "imdb_title_original": None,
            "imdb_title_original_status": "pending",
            "imdb_title_original_last_error": None,
        }
    elif stage == "omdb":
        updates = {
            "omdb_raw_json": None,
            "omdb_status": "pending",
            "omdb_last_error": None,
            "omdb_title": None,
            "omdb_year": None,
            "omdb_rated": None,
            "omdb_released": None,
            "omdb_runtime": None,
            "omdb_genre": None,
            "omdb_director": None,
            "omdb_writer": None,
            "omdb_actors": None,
            "omdb_plot_en": None,
            "omdb_plot_es": None,
            "omdb_language": None,
            "omdb_country": None,
            "omdb_awards": None,
            "omdb_poster": None,
            "omdb_imdbrating": None,
            "omdb_imdbvotes": None,
            "omdb_type": None,
            "omdb_dvd": None,
            "omdb_boxoffice": None,
            "omdb_production": None,
            "translation_status": "pending",
            "translation_last_error": None,
        }
    elif stage == "translation":
        updates = {
            "omdb_plot_es": None,
            "translation_status": "pending",
            "translation_last_error": None,
        }
    else:
        raise ValueError(f"Unknown stage: {stage}")

    updates["workflow_needs_review"] = False
    updates["workflow_review_reason"] = None
    updates["workflow_last_error"] = None

    _update_workflow_fields(movie_id, updates)
    _append_workflow_history(
        movie_id,
        event_type="reset",
        node=stage,
        message=f"Reset from stage {stage}",
    )



def _normalize_extensions(extensions: list[str] | None = None) -> set[str]:
    ext_set = {ext.lower() for ext in IMAGE_EXTENSIONS}
    if not extensions:
        return ext_set

    for extension in extensions:
        text = str(extension or "").strip().lower()
        if not text:
            continue
        if not text.startswith("."):
            text = f".{text}"
        ext_set.add(text)
    return ext_set


def _resolve_covers_dir(covers_dir: str | Path | None = None) -> Path:
    raw = DEFAULT_COVERS_DIR if covers_dir is None else covers_dir
    path = Path(raw).expanduser()
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path.resolve()


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


def _resolve_stored_path(path_text: str | None) -> Path | None:
    raw = str(path_text or "").strip()
    if not raw or raw.startswith(("http://", "https://")):
        return None
    if _is_windows_absolute_path(raw) and not Path(raw).is_absolute():
        return None
    path = Path(raw.replace("\\", "/")).expanduser()
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path.resolve()


def _project_cover_path(path: str | Path) -> Path:
    resolved = Path(path).expanduser().resolve()
    try:
        resolved.relative_to(PROJECT_ROOT)
        return resolved
    except ValueError:
        return (DEFAULT_COVERS_DIR / resolved.name).resolve()


def _cover_inside_project(path: str | Path) -> Path:
    resolved = Path(path).expanduser().resolve()
    destination = _project_cover_path(resolved)
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination != resolved:
        shutil.copy2(resolved, destination)
    return destination


def _path_candidates_from_text(path_text: str | None) -> list[Path]:
    raw = str(path_text or "").strip()
    if not raw:
        return []

    variants: list[str] = []
    seen: set[str] = set()

    def _push(text: str) -> None:
        clean = str(text or "").strip()
        if clean and clean not in seen:
            seen.add(clean)
            variants.append(clean)

    _push(raw)
    slash = raw.replace("\\", "/")
    _push(slash)

    if len(slash) >= 3 and slash[1] == ":" and slash[2] == "/":
        _push(slash[2:])

    win_parts = list(PureWindowsPath(raw).parts)
    if win_parts:
        if win_parts[0] in {"\\", "/"}:
            _push("/" + "/".join(win_parts[1:]))
        elif len(win_parts) >= 2 and win_parts[0].endswith(":"):
            _push("/" + "/".join(win_parts[1:]))

    out: list[Path] = []
    seen_paths: set[str] = set()
    for item in variants:
        try:
            candidate = _resolve_stored_path(item)
        except Exception:
            continue
        if candidate is None:
            continue
        key = candidate.as_posix()
        if key not in seen_paths:
            seen_paths.add(key)
            out.append(candidate)
    return out


def _first_existing_path(candidates: list[Path]) -> Path | None:
    for candidate in candidates:
        try:
            if candidate.exists() and candidate.is_file():
                return candidate.resolve()
        except OSError:
            continue
    return None


def _basename_candidates(image_path: str | None, image_filename: str | None) -> list[str]:
    names: list[str] = []
    seen: set[str] = set()

    def _push(name: str | None) -> None:
        text = str(name or "").strip()
        if text and text not in seen:
            seen.add(text)
            names.append(text)

    _push(image_filename)
    if image_path:
        raw = str(image_path).strip()
        _push(PureWindowsPath(raw).name)
        normalized = raw.replace("\\", "/")
        if normalized:
            _push(normalized.rsplit("/", 1)[-1])
    return names


def _portable_cover_path(
    *,
    movie_id: str,
    image_path: str | None,
    image_filename: str | None,
    covers_dir: Path,
    recursive: bool,
    extensions: set[str],
) -> str | None:
    resolved = _resolve_local_cover_path(
        movie_id=movie_id,
        image_path=image_path,
        image_filename=image_filename,
        covers_dir=covers_dir,
        recursive=recursive,
        extensions=extensions,
    )
    if resolved is not None:
        return _stored_image_path(_cover_inside_project(resolved))

    current = str(image_path or "").strip()
    names = _basename_candidates(current, image_filename)
    if not names:
        return None

    if Path(current).is_absolute() or _is_windows_absolute_path(current):
        return _stored_image_path(DEFAULT_COVERS_DIR / names[0])

    return current.replace("\\", "/")


def _normalize_stored_image_paths(con) -> int:
    ext_set = _normalize_extensions()
    rows = con.execute(
        f"""
        SELECT id, image_path, image_filename
        FROM {CORE_TABLE}
        WHERE image_path IS NOT NULL
          AND TRIM(image_path) <> ''
        """
    ).fetchall()

    updated = 0
    for movie_id, raw_path, image_filename in rows:
        current = str(raw_path or "").strip()
        normalized = _portable_cover_path(
            movie_id=str(movie_id or "").strip(),
            image_path=current,
            image_filename=str(image_filename or "").strip(),
            covers_dir=DEFAULT_COVERS_DIR,
            recursive=True,
            extensions=ext_set,
        )
        if normalized and normalized != current:
            con.execute(
                f"""
                UPDATE {CORE_TABLE}
                SET image_path = ?, updated_at = now()
                WHERE id = ?
                """,
                (normalized, movie_id),
            )
            updated += 1
    return updated

def _resolve_local_cover_path(
    *,
    movie_id: str,
    image_path: str | None,
    image_filename: str | None,
    covers_dir: Path,
    recursive: bool,
    extensions: set[str],
    filename_index: dict[str, Path] | None = None,
    prefer_existing_path: bool = True,
) -> Path | None:
    if prefer_existing_path:
        existing = _first_existing_path(_path_candidates_from_text(image_path))
        if existing is not None:
            return existing

    name_candidates = _basename_candidates(image_path, image_filename)
    for extension in sorted(extensions):
        name_candidates.append(f"{movie_id}{extension}")

    deduped_names: list[str] = []
    seen_names: set[str] = set()
    for name in name_candidates:
        clean = str(name or "").strip()
        if not clean:
            continue
        lowered = clean.lower()
        if lowered in seen_names:
            continue
        seen_names.add(lowered)
        deduped_names.append(clean)

    if filename_index:
        for name in deduped_names:
            indexed = filename_index.get(name.lower())
            if indexed and indexed.exists() and indexed.is_file():
                return indexed.resolve()

    for name in deduped_names:
        candidate = covers_dir / name
        if candidate.exists() and candidate.is_file():
            return candidate.resolve()

    if recursive:
        pattern = f"**/{movie_id}.*"
        for candidate in sorted(covers_dir.glob(pattern)):
            if candidate.is_file() and candidate.suffix.lower() in extensions:
                return candidate.resolve()

    return None


def _is_valid_movie_id(movie_id: str | None) -> bool:
    text = str(movie_id or "").strip()
    return bool(VALID_MOVIE_ID_PATTERN.fullmatch(text))


def ensure_local_image_path(
    movie_id: str,
    *,
    covers_dir: str | Path | None = None,
    recursive: bool = True,
    extensions: list[str] | None = None,
) -> str | None:
    con = get_connection()
    row = con.execute(
        """
        SELECT image_path, image_filename
        FROM movies_core
        WHERE id = ?
        """,
        (movie_id,),
    ).fetchone()

    if row is None:
        con.close()
        return None

    current_path = str(row[0] or "").strip()
    current_filename = str(row[1] or "").strip()

    ext_set = _normalize_extensions(extensions)
    resolved = _resolve_local_cover_path(
        movie_id=movie_id,
        image_path=current_path,
        image_filename=current_filename,
        covers_dir=_resolve_covers_dir(covers_dir),
        recursive=recursive,
        extensions=ext_set,
    )
    if resolved is None:
        con.close()
        return None

    storage_path = _cover_inside_project(resolved)
    stored_path = _stored_image_path(storage_path)
    resolved_path = storage_path.as_posix()
    resolved_filename = storage_path.name
    if stored_path != current_path or resolved_filename != current_filename:
        con.execute(
            f"""
            UPDATE {CORE_TABLE}
            SET
                image_path = ?,
                image_filename = ?,
                updated_at = now()
            WHERE id = ?
            """,
            (stored_path, resolved_filename, movie_id),
        )
    con.close()
    return resolved_path


def audit_cover_name_format(
    *,
    covers_dir: str | Path | None = None,
    recursive: bool = True,
    extensions: list[str] | None = None,
    db_limit: int = 200000,
) -> dict[str, Any]:
    if db_limit < 1:
        raise ValueError("db_limit must be >= 1")

    folder = _resolve_covers_dir(covers_dir)
    if not folder.exists() or not folder.is_dir():
        raise ValueError(f"Invalid folder: {folder}")

    ext_set = _normalize_extensions(extensions)
    pattern = "**/*" if recursive else "*"

    total_files = 0
    valid_files = 0
    invalid_files_preview: list[dict[str, str]] = []
    invalid_file_ids: list[str] = []
    seen_invalid_ids: set[str] = set()

    for path in sorted(folder.glob(pattern)):
        if not path.is_file() or path.suffix.lower() not in ext_set:
            continue

        total_files += 1
        stem = path.stem.strip()
        if _is_valid_movie_id(stem):
            valid_files += 1
            continue

        if stem not in seen_invalid_ids:
            seen_invalid_ids.add(stem)
            invalid_file_ids.append(stem)

        if len(invalid_files_preview) < 200:
            invalid_files_preview.append(
                {
                    "filename": path.name,
                    "stem": stem,
                    "expected": "PNNNN",
                }
            )

    con = get_connection()
    db_ids = [
        str(row[0] or "").strip()
        for row in con.execute(
            """
            SELECT id
            FROM movies
            ORDER BY LOWER(id), id
            LIMIT ?
            """,
            (db_limit,),
        ).fetchall()
    ]
    con.close()

    invalid_db_ids = [movie_id for movie_id in db_ids if movie_id and not _is_valid_movie_id(movie_id)]
    legacy_leading_zero_ids = [movie_id for movie_id in invalid_db_ids if LEGACY_LEADING_ZERO_PATTERN.fullmatch(movie_id)]

    return {
        "covers_dir": str(folder),
        "recursive": bool(recursive),
        "total_cover_files": total_files,
        "valid_cover_files": valid_files,
        "invalid_cover_files_count": total_files - valid_files,
        "invalid_cover_files_preview": invalid_files_preview,
        "invalid_cover_ids_unique_count": len(invalid_file_ids),
        "invalid_cover_ids_unique": invalid_file_ids[:200],
        "db_checked": len(db_ids),
        "invalid_db_ids_count": len(invalid_db_ids),
        "invalid_db_ids_preview": invalid_db_ids[:200],
        "legacy_leading_zero_ids_count": len(legacy_leading_zero_ids),
        "legacy_leading_zero_ids_preview": legacy_leading_zero_ids[:200],
        "expected_pattern": "PNNNN (P + 4 digitos 0-9)",
    }



def ingest_covers(
    folder: str,
    recursive: bool = True,
    extensions: list[str] | None = None,
    overwrite_existing_paths: bool = False,
) -> dict[str, Any]:
    ext_set = _normalize_extensions(extensions)

    folder_path = Path(folder).expanduser().resolve()
    if not folder_path.exists() or not folder_path.is_dir():
        raise ValueError(f"Invalid folder: {folder}")

    pattern = "**/*" if recursive else "*"
    files = sorted(
        [
            p
            for p in folder_path.glob(pattern)
            if p.is_file() and p.suffix.lower() in ext_set
        ]
    )

    con = get_connection()

    inserted = 0
    updated = 0
    skipped = 0
    copied_to_project = 0

    for path in files:
        movie_id = path.stem
        source_path = path.resolve()
        storage_path = _project_cover_path(source_path)
        stored_path = _stored_image_path(storage_path)
        stored_filename = storage_path.name

        row = con.execute(
            f"SELECT image_path FROM {CORE_TABLE} WHERE id = ?",
            (movie_id,),
        ).fetchone()

        def _copy_if_needed() -> None:
            nonlocal copied_to_project
            if storage_path != source_path:
                storage_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(source_path, storage_path)
                copied_to_project += 1

        if row is None:
            _copy_if_needed()
            con.execute(
                f"""
                INSERT INTO {CORE_TABLE} (id, image_path, image_filename, created_at, updated_at)
                VALUES (?, ?, ?, now(), now())
                """,
                (movie_id, stored_path, stored_filename),
            )
            _ensure_companion_rows_for_movie(con, movie_id)
            inserted += 1
            continue

        current_path = str(row[0] or "").strip()
        if current_path == stored_path:
            if not storage_path.exists():
                _copy_if_needed()
            skipped += 1
        elif overwrite_existing_paths:
            _copy_if_needed()
            con.execute(
                f"""
                UPDATE {CORE_TABLE}
                SET image_path = ?, image_filename = ?, updated_at = now()
                WHERE id = ?
                """,
                (stored_path, stored_filename, movie_id),
            )
            updated += 1
        else:
            skipped += 1

    con.close()

    return {
        "folder": str(folder_path),
        "found_files": len(files),
        "inserted": inserted,
        "updated": updated,
        "skipped": skipped,
        "copied_to_project": copied_to_project,
    }


def _row_to_dict(columns: list[str], row: tuple[Any, ...]) -> dict[str, Any]:
    data = dict(zip(columns, row))
    data.pop("extraction_title_model", None)
    data.pop("extraction_team_model", None)
    data.pop("translation_model", None)
    data["extraction_team"] = parse_json_list(data.pop("extraction_team_json", None))
    data["manual_team"] = parse_json_list(data.pop("manual_team_json", None))
    data["omdb_raw"] = _load_json(data.pop("omdb_raw_json", None))
    data["workflow_history"] = _load_json(data.pop("workflow_history_json", None))
    data["pipeline_stage"] = _derive_pipeline_stage_from_dict(data)
    return data



def list_movies(stage: str | None = None, limit: int = 500) -> list[dict[str, Any]]:
    con = get_connection()

    where = ""
    pipeline_filter: str | None = None
    if stage == "needs_extraction":
        where = f"WHERE {_MISSING_EXTRACTION_SQL}"
    elif stage == "needs_manual_review":
        where = "WHERE manual_title IS NULL OR manual_team_json IS NULL"
    elif stage == "needs_imdb":
        where = f"""
        WHERE imdb_url IS NULL
          OR imdb_url = ''
          OR (
                {_EFFECTIVE_TITLE_SQL} IS NOT NULL
            AND TRIM({_EFFECTIVE_TITLE_SQL}) <> ''
            AND STRPOS(TRIM({_EFFECTIVE_TITLE_SQL}), ';') > 0
            AND {_IMDB_URL_PARTS_SQL} <> {_TITLE_PARTS_SQL}
          )
        """
    elif stage == "needs_title_es":
        where = f"""
        WHERE imdb_url IS NOT NULL
          AND imdb_url <> ''
          AND (
                imdb_title_es IS NULL
             OR TRIM(imdb_title_es) = ''
             OR (
                    STRPOS(TRIM(imdb_url), ';') > 0
                AND {_IMDB_TITLE_ES_PARTS_SQL} <> {_IMDB_URL_PARTS_SQL}
             )
          )
        """
    elif stage == "needs_omdb":
        where = f"""
        WHERE imdb_id IS NOT NULL
          AND imdb_id <> ''
          AND (
                omdb_status IS NULL
             OR omdb_status <> 'fetched'
             OR (
                    STRPOS(TRIM(imdb_id), ';') > 0
                AND (
                       omdb_title IS NULL
                    OR TRIM(omdb_title) = ''
                    OR {_OMDB_TITLE_PARTS_SQL} <> {_IMDB_ID_PARTS_SQL}
                )
             )
          )
        """
    elif stage == "needs_translation":
        where = f"""
        WHERE omdb_plot_en IS NOT NULL
          AND omdb_plot_en <> ''
          AND (
                omdb_plot_es IS NULL
             OR omdb_plot_es = ''
             OR (
                    STRPOS(TRIM(omdb_plot_en), ';\n') > 0
                AND {_PLOT_ES_PARTS_SQL} <> {_PLOT_EN_PARTS_SQL}
             )
          )
        """
    elif stage == "needs_workflow_review":
        where = "WHERE workflow_needs_review = TRUE"
    elif stage == "pipeline_extraction":
        pipeline_filter = "extraction"
    elif stage == "pipeline_imdb":
        pipeline_filter = "imdb"
    elif stage == "pipeline_title_es":
        pipeline_filter = "title_es"
    elif stage == "pipeline_omdb":
        pipeline_filter = "omdb"
    elif stage == "pipeline_translation":
        pipeline_filter = "translation"
    elif stage == "pipeline_review":
        pipeline_filter = "review"
    elif stage == "pipeline_done":
        pipeline_filter = "done"

    rows = con.execute(
        f"""
        SELECT
            id,
            image_path,
            extraction_title,
            extraction_team_json,
            manual_title,
            manual_team_json,
            imdb_url,
            imdb_id,
            imdb_status,
            imdb_title_es,
            imdb_title_es_status,
            imdb_title_es_last_error,
            imdb_title_original,
            imdb_title_original_status,
            imdb_title_original_last_error,
            omdb_status,
            translation_status,
            omdb_plot_en,
            omdb_plot_es,
            workflow_status,
            workflow_current_node,
            workflow_needs_review,
            workflow_review_reason,
            workflow_attempt,
            workflow_last_error,
            updated_at
        FROM movies
        {where}
        ORDER BY LOWER(id), id
        LIMIT ?
        """,
        (limit,),
    ).fetchall()

    con.close()

    out: list[dict[str, Any]] = []
    for row in rows:
        out.append(
            {
                "id": row[0],
                "image_path": row[1],
                "extraction_title": row[2],
                "extraction_team": parse_json_list(row[3]),
                "manual_title": row[4],
                "manual_team": parse_json_list(row[5]),
                "imdb_url": row[6],
                "imdb_id": row[7],
                "imdb_status": row[8],
                "imdb_title_es": row[9],
                "imdb_title_es_status": row[10],
                "imdb_title_es_last_error": row[11],
                "imdb_title_original": row[12],
                "imdb_title_original_status": row[13],
                "imdb_title_original_last_error": row[14],
                "omdb_status": row[15],
                "translation_status": row[16],
                "omdb_plot_en": row[17],
                "omdb_plot_es": row[18],
                "workflow_status": row[19],
                "workflow_current_node": row[20],
                "workflow_needs_review": bool(row[21]) if row[21] is not None else False,
                "workflow_review_reason": row[22],
                "workflow_attempt": row[23],
                "workflow_last_error": row[24],
                "updated_at": row[25],
            }
        )

        out[-1]["pipeline_stage"] = _derive_pipeline_stage_from_dict(
            {
                "extraction_title": row[2],
                "extraction_team_json": row[3],
                "manual_title": row[4],
                "manual_team_json": row[5],
                "imdb_url": row[6],
                "imdb_title_es": row[9],
                "imdb_title_original": row[12],
                "omdb_status": row[15],
                "omdb_plot_en": row[17],
                "omdb_plot_es": row[18],
                "workflow_status": row[19],
                "workflow_current_node": row[20],
                "workflow_needs_review": bool(row[21]) if row[21] is not None else False,
            }
        )

    if pipeline_filter is not None:
        out = [row for row in out if str(row.get("pipeline_stage", "")).startswith(pipeline_filter)]

    return out



def get_movie(movie_id: str) -> dict[str, Any] | None:
    con = get_connection()
    row = con.execute("SELECT * FROM movies WHERE id = ?", (movie_id,)).fetchone()

    if row is None:
        con.close()
        return None

    columns = [d[1] for d in con.execute("PRAGMA table_info(movies)").fetchall()]
    con.close()

    return _row_to_dict(columns, row)



def get_stats() -> dict[str, int]:
    con = get_connection()

    total = con.execute("SELECT COUNT(*) FROM movies").fetchone()[0]
    needs_extraction = con.execute(
        f"SELECT COUNT(*) FROM movies WHERE {_MISSING_EXTRACTION_SQL}"
    ).fetchone()[0]
    needs_manual_review = con.execute(
        "SELECT COUNT(*) FROM movies WHERE manual_title IS NULL OR manual_team_json IS NULL"
    ).fetchone()[0]
    needs_imdb = con.execute(
        f"""
        SELECT COUNT(*) FROM movies
        WHERE imdb_url IS NULL
           OR imdb_url = ''
           OR (
                 {_EFFECTIVE_TITLE_SQL} IS NOT NULL
             AND TRIM({_EFFECTIVE_TITLE_SQL}) <> ''
             AND STRPOS(TRIM({_EFFECTIVE_TITLE_SQL}), ';') > 0
             AND {_IMDB_URL_PARTS_SQL} <> {_TITLE_PARTS_SQL}
           )
        """
    ).fetchone()[0]
    needs_title_es = con.execute(
        f"""
        SELECT COUNT(*) FROM movies
        WHERE imdb_url IS NOT NULL
          AND imdb_url <> ''
          AND (
                imdb_title_es IS NULL
             OR TRIM(imdb_title_es) = ''
             OR (
                    STRPOS(TRIM(imdb_url), ';') > 0
                AND {_IMDB_TITLE_ES_PARTS_SQL} <> {_IMDB_URL_PARTS_SQL}
             )
          )
        """
    ).fetchone()[0]
    needs_omdb = con.execute(
        f"""
        SELECT COUNT(*) FROM movies
        WHERE imdb_id IS NOT NULL
          AND imdb_id <> ''
          AND (
                omdb_status IS NULL
             OR omdb_status <> 'fetched'
             OR (
                    STRPOS(TRIM(imdb_id), ';') > 0
                AND (
                       omdb_title IS NULL
                    OR TRIM(omdb_title) = ''
                    OR {_OMDB_TITLE_PARTS_SQL} <> {_IMDB_ID_PARTS_SQL}
                )
             )
          )
        """
    ).fetchone()[0]
    needs_translation = con.execute(
        f"""
        SELECT COUNT(*) FROM movies
        WHERE omdb_plot_en IS NOT NULL
          AND omdb_plot_en <> ''
          AND (
                omdb_plot_es IS NULL
             OR omdb_plot_es = ''
             OR (
                    STRPOS(TRIM(omdb_plot_en), ';\n') > 0
                AND {_PLOT_ES_PARTS_SQL} <> {_PLOT_EN_PARTS_SQL}
             )
          )
        """
    ).fetchone()[0]
    needs_workflow_review = con.execute(
        "SELECT COUNT(*) FROM movies WHERE workflow_needs_review = TRUE"
    ).fetchone()[0]

    con.close()

    return {
        "total": total,
        "needs_extraction": needs_extraction,
        "needs_manual_review": needs_manual_review,
        "needs_imdb": needs_imdb,
        "needs_title_es": needs_title_es,
        "needs_omdb": needs_omdb,
        "needs_translation": needs_translation,
        "needs_workflow_review": needs_workflow_review,
    }



def update_title_team(movie_id: str, title: str | None, team: list[str]) -> None:
    _update_workflow_fields(
        movie_id,
        {
            "manual_title": title,
            "manual_team_json": _serialize_json(team),
            "workflow_status": "pending",
            "workflow_needs_review": False,
            "workflow_review_reason": None,
            "workflow_last_error": None,
        },
    )



def update_extraction(
    movie_id: str,
    *,
    title: str | None,
    team: list[str],
    title_raw: str,
    team_raw: str,
) -> None:
    _update_workflow_fields(
        movie_id,
        {
            "extraction_title": title,
            "extraction_team_json": _serialize_json(team),
            "extraction_title_raw": title_raw,
            "extraction_team_raw": team_raw,
            "workflow_status": "pending",
            "workflow_last_error": None,
        },
    )



def update_imdb(
    movie_id: str,
    *,
    imdb_query: str,
    imdb_url: str | None,
    imdb_status: str,
    imdb_last_error: str | None = None,
) -> None:
    canonical_urls: list[str] = []
    for raw_url in split_values(imdb_url):
        canonical = canonical_imdb_url(raw_url)
        if canonical:
            canonical_urls.append(canonical)

    canonical_url = join_values(canonical_urls) if canonical_urls else None
    if canonical_url and len(canonical_urls) == 1:
        canonical_url = canonical_urls[0]

    imdb_ids = [extract_imdb_id(url) for url in canonical_urls]
    imdb_id = join_values([item for item in imdb_ids if item]) if imdb_ids else None
    if imdb_id and len(imdb_ids) == 1:
        imdb_id = imdb_ids[0]

    _update_workflow_fields(
        movie_id,
        {
            "imdb_query": imdb_query,
            "imdb_url": canonical_url,
            "imdb_id": imdb_id,
            "imdb_status": imdb_status,
            "imdb_last_error": imdb_last_error,
            "imdb_title_es": None,
            "imdb_title_es_status": "pending",
            "imdb_title_es_last_error": None,
            "imdb_title_original": None,
            "imdb_title_original_status": "pending",
            "imdb_title_original_last_error": None,
            "omdb_raw_json": None,
            "omdb_status": "pending",
            "omdb_last_error": None,
            "omdb_title": None,
            "omdb_year": None,
            "omdb_rated": None,
            "omdb_released": None,
            "omdb_runtime": None,
            "omdb_genre": None,
            "omdb_director": None,
            "omdb_writer": None,
            "omdb_actors": None,
            "omdb_plot_en": None,
            "omdb_plot_es": None,
            "omdb_language": None,
            "omdb_country": None,
            "omdb_awards": None,
            "omdb_poster": None,
            "omdb_imdbrating": None,
            "omdb_imdbvotes": None,
            "omdb_type": None,
            "omdb_dvd": None,
            "omdb_boxoffice": None,
            "omdb_production": None,
            "translation_status": "pending",
            "translation_last_error": None,
            "workflow_status": "pending",
            "workflow_last_error": None,
        },
    )



def set_manual_imdb(movie_id: str, imdb_url: str) -> None:
    raw_urls = split_values(imdb_url)
    if not raw_urls:
        raise ValueError("Invalid IMDb URL")

    canonical_urls: list[str] = []
    for raw_url in raw_urls:
        canonical = canonical_imdb_url(raw_url)
        if not canonical:
            raise ValueError(f"Invalid IMDb URL: {raw_url}")
        canonical_urls.append(canonical)

    canonical_url = canonical_urls[0] if len(canonical_urls) == 1 else join_values(canonical_urls)
    imdb_ids = [extract_imdb_id(url) for url in canonical_urls]
    imdb_id = imdb_ids[0] if len(imdb_ids) == 1 else join_values(imdb_ids)

    _update_workflow_fields(
        movie_id,
        {
            "imdb_url": canonical_url,
            "imdb_id": imdb_id,
            "imdb_status": "found",
            "imdb_last_error": None,
            "imdb_title_es": None,
            "imdb_title_es_status": "pending",
            "imdb_title_es_last_error": None,
            "imdb_title_original": None,
            "imdb_title_original_status": "pending",
            "imdb_title_original_last_error": None,
            "omdb_raw_json": None,
            "omdb_status": "pending",
            "omdb_last_error": None,
            "omdb_title": None,
            "omdb_year": None,
            "omdb_rated": None,
            "omdb_released": None,
            "omdb_runtime": None,
            "omdb_genre": None,
            "omdb_director": None,
            "omdb_writer": None,
            "omdb_actors": None,
            "omdb_plot_en": None,
            "omdb_plot_es": None,
            "omdb_language": None,
            "omdb_country": None,
            "omdb_awards": None,
            "omdb_poster": None,
            "omdb_imdbrating": None,
            "omdb_imdbvotes": None,
            "omdb_type": None,
            "omdb_dvd": None,
            "omdb_boxoffice": None,
            "omdb_production": None,
            "translation_status": "pending",
            "translation_last_error": None,
            "workflow_status": "pending",
            "workflow_needs_review": False,
            "workflow_review_reason": None,
            "workflow_last_error": None,
        },
    )



def update_imdb_title_es(
    movie_id: str,
    *,
    title_es: str | None,
    status: str,
    error: str | None = None,
) -> None:
    _update_workflow_fields(
        movie_id,
        {
            "imdb_title_es": title_es,
            "imdb_title_es_status": status,
            "imdb_title_es_last_error": error,
            "imdb_title_original": None,
            "imdb_title_original_status": "pending",
            "imdb_title_original_last_error": None,
            "workflow_status": "pending",
            "workflow_last_error": None,
        },
    )


def set_manual_imdb_title_es(movie_id: str, title_es: str | None) -> None:
    clean = str(title_es or "").strip()
    if clean:
        update_imdb_title_es(movie_id, title_es=clean, status="manual", error=None)
    else:
        update_imdb_title_es(movie_id, title_es=None, status="pending", error=None)


def update_omdb(movie_id: str, omdb_payload: dict[str, Any], status: str, error: str | None) -> None:
    if status != "fetched":
        _update_workflow_fields(
            movie_id,
            {
                "omdb_status": status,
                "omdb_last_error": error,
                "workflow_status": "pending",
                "workflow_last_error": None,
            },
        )
        return

    _update_workflow_fields(
        movie_id,
        (
            {
                "omdb_raw_json": _serialize_json(omdb_payload),
                "omdb_status": "fetched",
                "omdb_last_error": None,
                "omdb_title": omdb_payload.get("Title"),
                "omdb_year": omdb_payload.get("Year"),
                "omdb_rated": omdb_payload.get("Rated"),
                "omdb_released": omdb_payload.get("Released"),
                "omdb_runtime": omdb_payload.get("Runtime"),
                "omdb_genre": translate_omdb_field(omdb_payload.get("Genre"), "Genre"),
                "omdb_director": omdb_payload.get("Director"),
                "omdb_writer": omdb_payload.get("Writer"),
                "omdb_actors": omdb_payload.get("Actors"),
                "omdb_plot_en": omdb_payload.get("Plot"),
                "omdb_language": translate_omdb_field(omdb_payload.get("Language"), "Language"),
                "omdb_country": translate_omdb_field(omdb_payload.get("Country"), "Country"),
                "omdb_awards": omdb_payload.get("Awards"),
                "omdb_poster": omdb_payload.get("Poster"),
                "omdb_imdbrating": omdb_payload.get("imdbRating"),
                "omdb_imdbvotes": omdb_payload.get("imdbVotes"),
                "omdb_type": translate_omdb_field(omdb_payload.get("Type"), "Type"),
                "omdb_dvd": omdb_payload.get("DVD"),
                "omdb_boxoffice": omdb_payload.get("BoxOffice"),
                "omdb_production": omdb_payload.get("Production"),
                "workflow_status": "pending",
                "workflow_last_error": None,
            }
        ),
    )



def update_omdb_fields(movie_id: str, fields: dict[str, Any]) -> None:
    allowed = {
        "omdb_title",
        "omdb_year",
        "omdb_runtime",
        "omdb_genre",
        "omdb_director",
        "omdb_writer",
        "omdb_actors",
        "omdb_plot_en",
        "omdb_plot_es",
        "omdb_language",
        "omdb_country",
        "omdb_type",
    }

    clean = translate_omdb_fields({k: v for k, v in fields.items() if k in allowed})
    if not clean:
        return

    _update_workflow_fields(movie_id, clean)



def update_plot_translation(
    movie_id: str,
    *,
    plot_es: str | None,
    status: str,
    error: str | None = None,
) -> None:
    con = get_connection()
    row = con.execute(
        "SELECT omdb_plot_en FROM movies WHERE id = ?",
        (movie_id,),
    ).fetchone()
    con.close()
    plot_en = row[0] if row else None
    normalized_plot_es = _normalize_plot_es_text(plot_en, plot_es)
    _update_workflow_fields(
        movie_id,
        {
            "omdb_plot_es": normalized_plot_es,
            "translation_status": status,
            "translation_last_error": error,
            "workflow_status": "pending",
            "workflow_last_error": None,
        },
    )



def movies_for_extraction(limit: int, overwrite: bool) -> list[dict[str, str]]:
    con = get_connection()
    where = ""
    if not overwrite:
        where = f"WHERE {_MISSING_EXTRACTION_SQL}"

    rows = con.execute(
        f"""
        SELECT id, image_path
        FROM movies
        {where}
        ORDER BY LOWER(id), id
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    con.close()

    return [{"id": row[0], "image_path": row[1]} for row in rows]



def movies_for_imdb(limit: int, overwrite: bool) -> list[dict[str, Any]]:
    con = get_connection()
    where = ""
    if not overwrite:
        where = "WHERE imdb_url IS NULL OR imdb_url = ''"

    rows = con.execute(
        f"""
        SELECT
            id,
            extraction_title,
            extraction_team_json,
            manual_title,
            manual_team_json
        FROM movies
        {where}
        ORDER BY LOWER(id), id
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    con.close()

    output: list[dict[str, Any]] = []
    for row in rows:
        output.append(
            {
                "id": row[0],
                "extraction_title": row[1],
                "extraction_team": parse_json_list(row[2]),
                "manual_title": row[3],
                "manual_team": parse_json_list(row[4]),
            }
        )
    return output



def movies_for_imdb_title_es(limit: int, overwrite: bool) -> list[dict[str, Any]]:
    con = get_connection()
    where = "WHERE imdb_url IS NOT NULL AND imdb_url <> ''"
    if not overwrite:
        where += f"""
        AND (
              imdb_title_es IS NULL
           OR TRIM(imdb_title_es) = ''
           OR (
                  STRPOS(TRIM(imdb_url), ';') > 0
              AND {_IMDB_TITLE_ES_PARTS_SQL} <> {_IMDB_URL_PARTS_SQL}
           )
        )
        """

    rows = con.execute(
        f"""
        SELECT id, imdb_url, imdb_id
        FROM movies
        {where}
        ORDER BY LOWER(id), id
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    con.close()

    return [{"id": row[0], "imdb_url": row[1], "imdb_id": row[2]} for row in rows]


def movies_for_omdb(limit: int, overwrite: bool) -> list[dict[str, Any]]:
    con = get_connection()
    where = "WHERE imdb_id IS NOT NULL AND imdb_id <> ''"
    if not overwrite:
        where += f"""
        AND (
              omdb_status IS NULL
           OR omdb_status <> 'fetched'
           OR (
                  STRPOS(TRIM(imdb_id), ';') > 0
              AND (
                     omdb_title IS NULL
                  OR TRIM(omdb_title) = ''
                  OR {_OMDB_TITLE_PARTS_SQL} <> {_IMDB_ID_PARTS_SQL}
              )
           )
        )
        """

    rows = con.execute(
        f"""
        SELECT id, imdb_id
        FROM movies
        {where}
        ORDER BY LOWER(id), id
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    con.close()

    return [{"id": row[0], "imdb_id": row[1]} for row in rows]



def movies_for_translation(limit: int, overwrite: bool) -> list[dict[str, Any]]:
    con = get_connection()
    where = "WHERE omdb_plot_en IS NOT NULL AND omdb_plot_en <> ''"
    if not overwrite:
        where += f"""
        AND (
              omdb_plot_es IS NULL
           OR omdb_plot_es = ''
           OR (
                  STRPOS(TRIM(omdb_plot_en), ';\n') > 0
              AND {_PLOT_ES_PARTS_SQL} <> {_PLOT_EN_PARTS_SQL}
           )
        )
        """

    rows = con.execute(
        f"""
        SELECT id, omdb_plot_en
        FROM movies
        {where}
        ORDER BY LOWER(id), id
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    con.close()

    return [{"id": row[0], "omdb_plot_en": row[1]} for row in rows]



def movie_ids_for_workflow(
    *,
    limit: int,
    start_stage: str = "extraction",
    overwrite: bool = False,
) -> list[str]:
    stage = start_stage.lower().strip()

    con = get_connection()
    if overwrite:
        where = ""
    elif stage == "extraction":
        where = f"WHERE {_MISSING_EXTRACTION_SQL} OR workflow_needs_review = TRUE"
    elif stage == "imdb":
        where = f"""
        WHERE imdb_url IS NULL
           OR imdb_url = ''
           OR (
                 {_EFFECTIVE_TITLE_SQL} IS NOT NULL
             AND TRIM({_EFFECTIVE_TITLE_SQL}) <> ''
             AND STRPOS(TRIM({_EFFECTIVE_TITLE_SQL}), ';') > 0
             AND {_IMDB_URL_PARTS_SQL} <> {_TITLE_PARTS_SQL}
           )
           OR workflow_needs_review = TRUE
        """
    elif stage == "title_es":
        where = f"""
        WHERE imdb_url IS NOT NULL
          AND imdb_url <> ''
          AND (
                (imdb_title_es IS NULL OR TRIM(imdb_title_es) = '')
             OR (
                    STRPOS(TRIM(imdb_url), ';') > 0
                AND {_IMDB_TITLE_ES_PARTS_SQL} <> {_IMDB_URL_PARTS_SQL}
             )
             OR workflow_needs_review = TRUE
          )
        """
    elif stage == "omdb":
        where = f"""
        WHERE imdb_id IS NOT NULL
          AND imdb_id <> ''
          AND (
                (omdb_status IS NULL OR omdb_status <> 'fetched')
             OR (
                    STRPOS(TRIM(imdb_id), ';') > 0
                AND (
                       omdb_title IS NULL
                    OR TRIM(omdb_title) = ''
                    OR {_OMDB_TITLE_PARTS_SQL} <> {_IMDB_ID_PARTS_SQL}
                )
             )
             OR workflow_needs_review = TRUE
          )
        """
    elif stage == "translation":
        where = f"""
        WHERE omdb_plot_en IS NOT NULL
          AND omdb_plot_en <> ''
          AND (
                (omdb_plot_es IS NULL OR omdb_plot_es = '')
             OR (
                    STRPOS(TRIM(omdb_plot_en), ';\n') > 0
                AND {_PLOT_ES_PARTS_SQL} <> {_PLOT_EN_PARTS_SQL}
             )
             OR workflow_needs_review = TRUE
          )
        """
    else:
        where = "WHERE workflow_status IS NULL OR workflow_status <> 'done'"

    rows = con.execute(
        f"""
        SELECT id
        FROM movies
        {where}
        ORDER BY LOWER(id), id
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    con.close()

    return [row[0] for row in rows]
