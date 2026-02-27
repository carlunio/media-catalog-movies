import json
from datetime import datetime
from pathlib import Path
from typing import Any

from ..database import get_connection
from ..normalizers import (
    canonical_imdb_url,
    ensure_abs_path,
    extract_imdb_id,
    parse_json_list,
)

WORKFLOW_STAGE_ORDER = {
    "extraction": 1,
    "imdb": 2,
    "omdb": 3,
    "translation": 4,
}


def _derive_pipeline_stage_from_dict(movie: dict[str, Any]) -> str:
    if bool(movie.get("workflow_needs_review")):
        return "review"

    workflow_status = str(movie.get("workflow_status") or "").lower()
    workflow_node = str(movie.get("workflow_current_node") or "").strip()
    if workflow_status == "running":
        return f"running:{workflow_node}" if workflow_node else "running"

    extraction_title = str(movie.get("extraction_title") or "").strip()
    extraction_team = parse_json_list(movie.get("extraction_team_json"))
    if not extraction_title or not extraction_team:
        return "extraction"

    imdb_url = str(movie.get("imdb_url") or "").strip()
    if not imdb_url:
        return "imdb"

    omdb_status = str(movie.get("omdb_status") or "").lower()
    if omdb_status != "fetched":
        return "omdb"

    omdb_plot_en = str(movie.get("omdb_plot_en") or "").strip()
    omdb_plot_es = str(movie.get("omdb_plot_es") or "").strip()
    if omdb_plot_en and not omdb_plot_es:
        return "translation"

    return "done"


def init_table() -> None:
    con = get_connection()
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS movies (
            id TEXT PRIMARY KEY,
            image_path TEXT NOT NULL,
            image_filename TEXT,

            extraction_title TEXT,
            extraction_team_json JSON,
            extraction_title_raw TEXT,
            extraction_team_raw TEXT,
            extraction_title_model TEXT,
            extraction_team_model TEXT,

            manual_title TEXT,
            manual_team_json JSON,

            imdb_query TEXT,
            imdb_url TEXT,
            imdb_id TEXT,
            imdb_status TEXT DEFAULT 'pending',
            imdb_last_error TEXT,

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
            translation_model TEXT,
            translation_last_error TEXT,

            workflow_status TEXT DEFAULT 'pending',
            workflow_current_node TEXT,
            workflow_needs_review BOOLEAN DEFAULT FALSE,
            workflow_review_reason TEXT,
            workflow_attempt INTEGER DEFAULT 0,
            workflow_last_action TEXT,
            workflow_last_error TEXT,
            workflow_history_json JSON,

            created_at TIMESTAMP DEFAULT now(),
            updated_at TIMESTAMP DEFAULT now()
        )
        """
    )

    _ensure_columns(con)
    con.close()



def _ensure_columns(con) -> None:
    existing = {row[1] for row in con.execute("PRAGMA table_info(movies)").fetchall()}
    required_columns = {
        "workflow_status": "TEXT DEFAULT 'pending'",
        "workflow_current_node": "TEXT",
        "workflow_needs_review": "BOOLEAN DEFAULT FALSE",
        "workflow_review_reason": "TEXT",
        "workflow_attempt": "INTEGER DEFAULT 0",
        "workflow_last_action": "TEXT",
        "workflow_last_error": "TEXT",
        "workflow_history_json": "JSON",
    }

    for col, ddl in required_columns.items():
        if col not in existing:
            con.execute(f"ALTER TABLE movies ADD COLUMN {col} {ddl}")



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
    row = con.execute(
        "SELECT workflow_history_json FROM movies WHERE id = ?",
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
        """
        UPDATE movies
        SET
            workflow_history_json = ?,
            updated_at = now()
        WHERE id = ?
        """,
        (_serialize_json(history), movie_id),
    )
    con.close()



def _update_workflow_fields(movie_id: str, fields: dict[str, Any]) -> None:
    clean_fields = {k: v for k, v in fields.items()}
    if not clean_fields:
        return

    assignments = ", ".join(f"{col} = ?" for col in clean_fields)
    values = list(clean_fields.values())

    con = get_connection()
    con.execute(
        f"UPDATE movies SET {assignments}, updated_at = now() WHERE id = ?",
        values + [movie_id],
    )
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



def increment_workflow_attempt(movie_id: str) -> int:
    con = get_connection()
    row = con.execute(
        "SELECT COALESCE(workflow_attempt, 0) FROM movies WHERE id = ?",
        (movie_id,),
    ).fetchone()
    current = int(row[0]) if row else 0
    updated = current + 1

    con.execute(
        "UPDATE movies SET workflow_attempt = ?, updated_at = now() WHERE id = ?",
        (updated, movie_id),
    )
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
            "extraction_title_model": None,
            "extraction_team_model": None,
            "manual_title": None,
            "manual_team_json": None,
            "imdb_query": None,
            "imdb_url": None,
            "imdb_id": None,
            "imdb_status": "pending",
            "imdb_last_error": None,
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
            "translation_model": None,
            "translation_last_error": None,
        }
    elif stage == "imdb":
        updates = {
            "imdb_query": None,
            "imdb_url": None,
            "imdb_id": None,
            "imdb_status": "pending",
            "imdb_last_error": None,
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
            "translation_model": None,
            "translation_last_error": None,
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
            "translation_model": None,
            "translation_last_error": None,
        }
    elif stage == "translation":
        updates = {
            "omdb_plot_es": None,
            "translation_status": "pending",
            "translation_model": None,
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



def ingest_covers(
    folder: str,
    recursive: bool = True,
    extensions: list[str] | None = None,
    overwrite_existing_paths: bool = False,
) -> dict[str, Any]:
    ext_set = {".jpg", ".jpeg", ".png", ".heic", ".webp"}
    if extensions:
        ext_set = {e.lower() if e.startswith(".") else f".{e.lower()}" for e in extensions}

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

    for path in files:
        movie_id = path.stem
        abs_path = ensure_abs_path(path)

        row = con.execute(
            "SELECT image_path FROM movies WHERE id = ?",
            (movie_id,),
        ).fetchone()

        if row is None:
            con.execute(
                """
                INSERT INTO movies (id, image_path, image_filename, created_at, updated_at)
                VALUES (?, ?, ?, now(), now())
                """,
                (movie_id, abs_path, path.name),
            )
            inserted += 1
            continue

        current_path = row[0]
        if overwrite_existing_paths and current_path != abs_path:
            con.execute(
                """
                UPDATE movies
                SET image_path = ?, image_filename = ?, updated_at = now()
                WHERE id = ?
                """,
                (abs_path, path.name, movie_id),
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
    }



def _row_to_dict(columns: list[str], row: tuple[Any, ...]) -> dict[str, Any]:
    data = dict(zip(columns, row))
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
        where = "WHERE extraction_title IS NULL OR extraction_team_json IS NULL"
    elif stage == "needs_manual_review":
        where = "WHERE manual_title IS NULL OR manual_team_json IS NULL"
    elif stage == "needs_imdb":
        where = "WHERE imdb_url IS NULL OR imdb_url = ''"
    elif stage == "needs_omdb":
        where = "WHERE imdb_id IS NOT NULL AND imdb_id <> '' AND (omdb_status IS NULL OR omdb_status <> 'fetched')"
    elif stage == "needs_translation":
        where = "WHERE omdb_plot_en IS NOT NULL AND omdb_plot_en <> '' AND (omdb_plot_es IS NULL OR omdb_plot_es = '')"
    elif stage == "needs_workflow_review":
        where = "WHERE workflow_needs_review = TRUE"
    elif stage == "pipeline_extraction":
        pipeline_filter = "extraction"
    elif stage == "pipeline_imdb":
        pipeline_filter = "imdb"
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
        ORDER BY id
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
                "omdb_status": row[9],
                "translation_status": row[10],
                "omdb_plot_en": row[11],
                "omdb_plot_es": row[12],
                "workflow_status": row[13],
                "workflow_current_node": row[14],
                "workflow_needs_review": bool(row[15]) if row[15] is not None else False,
                "workflow_review_reason": row[16],
                "workflow_attempt": row[17],
                "workflow_last_error": row[18],
                "updated_at": row[19],
            }
        )

        out[-1]["pipeline_stage"] = _derive_pipeline_stage_from_dict(
            {
                "extraction_title": row[2],
                "extraction_team_json": row[3],
                "imdb_url": row[6],
                "omdb_status": row[9],
                "omdb_plot_en": row[11],
                "omdb_plot_es": row[12],
                "workflow_status": row[13],
                "workflow_current_node": row[14],
                "workflow_needs_review": bool(row[15]) if row[15] is not None else False,
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
        "SELECT COUNT(*) FROM movies WHERE extraction_title IS NULL OR extraction_team_json IS NULL"
    ).fetchone()[0]
    needs_manual_review = con.execute(
        "SELECT COUNT(*) FROM movies WHERE manual_title IS NULL OR manual_team_json IS NULL"
    ).fetchone()[0]
    needs_imdb = con.execute(
        "SELECT COUNT(*) FROM movies WHERE imdb_url IS NULL OR imdb_url = ''"
    ).fetchone()[0]
    needs_omdb = con.execute(
        """
        SELECT COUNT(*) FROM movies
        WHERE imdb_id IS NOT NULL
          AND imdb_id <> ''
          AND (omdb_status IS NULL OR omdb_status <> 'fetched')
        """
    ).fetchone()[0]
    needs_translation = con.execute(
        """
        SELECT COUNT(*) FROM movies
        WHERE omdb_plot_en IS NOT NULL
          AND omdb_plot_en <> ''
          AND (omdb_plot_es IS NULL OR omdb_plot_es = '')
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
        "needs_omdb": needs_omdb,
        "needs_translation": needs_translation,
        "needs_workflow_review": needs_workflow_review,
    }



def update_title_team(movie_id: str, title: str | None, team: list[str]) -> None:
    con = get_connection()
    con.execute(
        """
        UPDATE movies
        SET
            manual_title = ?,
            manual_team_json = ?,
            workflow_status = 'pending',
            workflow_needs_review = FALSE,
            workflow_review_reason = NULL,
            updated_at = now()
        WHERE id = ?
        """,
        (title, _serialize_json(team), movie_id),
    )
    con.close()



def update_extraction(
    movie_id: str,
    *,
    title: str | None,
    team: list[str],
    title_raw: str,
    team_raw: str,
    title_model: str,
    team_model: str,
) -> None:
    con = get_connection()
    con.execute(
        """
        UPDATE movies
        SET
            extraction_title = ?,
            extraction_team_json = ?,
            extraction_title_raw = ?,
            extraction_team_raw = ?,
            extraction_title_model = ?,
            extraction_team_model = ?,
            workflow_status = 'pending',
            workflow_last_error = NULL,
            updated_at = now()
        WHERE id = ?
        """,
        (
            title,
            _serialize_json(team),
            title_raw,
            team_raw,
            title_model,
            team_model,
            movie_id,
        ),
    )
    con.close()



def update_imdb(
    movie_id: str,
    *,
    imdb_query: str,
    imdb_url: str | None,
    imdb_status: str,
    imdb_last_error: str | None = None,
) -> None:
    canonical_url = canonical_imdb_url(imdb_url) if imdb_url else None
    imdb_id = extract_imdb_id(canonical_url or imdb_url)

    con = get_connection()
    con.execute(
        """
        UPDATE movies
        SET
            imdb_query = ?,
            imdb_url = ?,
            imdb_id = ?,
            imdb_status = ?,
            imdb_last_error = ?,
            workflow_status = 'pending',
            workflow_last_error = NULL,
            updated_at = now()
        WHERE id = ?
        """,
        (imdb_query, canonical_url, imdb_id, imdb_status, imdb_last_error, movie_id),
    )
    con.close()



def set_manual_imdb(movie_id: str, imdb_url: str) -> None:
    canonical_url = canonical_imdb_url(imdb_url)
    if not canonical_url:
        raise ValueError("Invalid IMDb URL")

    con = get_connection()
    con.execute(
        """
        UPDATE movies
        SET
            imdb_url = ?,
            imdb_id = ?,
            imdb_status = 'found',
            imdb_last_error = NULL,
            workflow_status = 'pending',
            workflow_needs_review = FALSE,
            workflow_review_reason = NULL,
            workflow_last_error = NULL,
            updated_at = now()
        WHERE id = ?
        """,
        (canonical_url, extract_imdb_id(canonical_url), movie_id),
    )
    con.close()



def update_omdb(movie_id: str, omdb_payload: dict[str, Any], status: str, error: str | None) -> None:
    con = get_connection()

    if status != "fetched":
        con.execute(
            """
            UPDATE movies
            SET
                omdb_status = ?,
                omdb_last_error = ?,
                workflow_status = 'pending',
                workflow_last_error = NULL,
                updated_at = now()
            WHERE id = ?
            """,
            (status, error, movie_id),
        )
        con.close()
        return

    con.execute(
        """
        UPDATE movies
        SET
            omdb_raw_json = ?,
            omdb_status = 'fetched',
            omdb_last_error = NULL,
            omdb_title = ?,
            omdb_year = ?,
            omdb_rated = ?,
            omdb_released = ?,
            omdb_runtime = ?,
            omdb_genre = ?,
            omdb_director = ?,
            omdb_writer = ?,
            omdb_actors = ?,
            omdb_plot_en = ?,
            omdb_language = ?,
            omdb_country = ?,
            omdb_awards = ?,
            omdb_poster = ?,
            omdb_imdbrating = ?,
            omdb_imdbvotes = ?,
            omdb_type = ?,
            omdb_dvd = ?,
            omdb_boxoffice = ?,
            omdb_production = ?,
            workflow_status = 'pending',
            workflow_last_error = NULL,
            updated_at = now()
        WHERE id = ?
        """,
        (
            _serialize_json(omdb_payload),
            omdb_payload.get("Title"),
            omdb_payload.get("Year"),
            omdb_payload.get("Rated"),
            omdb_payload.get("Released"),
            omdb_payload.get("Runtime"),
            omdb_payload.get("Genre"),
            omdb_payload.get("Director"),
            omdb_payload.get("Writer"),
            omdb_payload.get("Actors"),
            omdb_payload.get("Plot"),
            omdb_payload.get("Language"),
            omdb_payload.get("Country"),
            omdb_payload.get("Awards"),
            omdb_payload.get("Poster"),
            omdb_payload.get("imdbRating"),
            omdb_payload.get("imdbVotes"),
            omdb_payload.get("Type"),
            omdb_payload.get("DVD"),
            omdb_payload.get("BoxOffice"),
            omdb_payload.get("Production"),
            movie_id,
        ),
    )

    con.close()



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

    clean = {k: v for k, v in fields.items() if k in allowed}
    if not clean:
        return

    assignments = ", ".join(f"{k} = ?" for k in clean)
    values = list(clean.values())

    con = get_connection()
    con.execute(
        f"UPDATE movies SET {assignments}, updated_at = now() WHERE id = ?",
        values + [movie_id],
    )
    con.close()



def update_plot_translation(
    movie_id: str,
    *,
    plot_es: str | None,
    model: str | None,
    status: str,
    error: str | None = None,
) -> None:
    con = get_connection()
    con.execute(
        """
        UPDATE movies
        SET
            omdb_plot_es = ?,
            translation_model = ?,
            translation_status = ?,
            translation_last_error = ?,
            workflow_status = 'pending',
            workflow_last_error = NULL,
            updated_at = now()
        WHERE id = ?
        """,
        (plot_es, model, status, error, movie_id),
    )
    con.close()



def movies_for_extraction(limit: int, overwrite: bool) -> list[dict[str, str]]:
    con = get_connection()
    where = ""
    if not overwrite:
        where = "WHERE extraction_title IS NULL OR extraction_team_json IS NULL"

    rows = con.execute(
        f"""
        SELECT id, image_path
        FROM movies
        {where}
        ORDER BY id
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
        ORDER BY id
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



def movies_for_omdb(limit: int, overwrite: bool) -> list[dict[str, Any]]:
    con = get_connection()
    where = "WHERE imdb_id IS NOT NULL AND imdb_id <> ''"
    if not overwrite:
        where += " AND (omdb_status IS NULL OR omdb_status <> 'fetched')"

    rows = con.execute(
        f"""
        SELECT id, imdb_id
        FROM movies
        {where}
        ORDER BY id
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
        where += " AND (omdb_plot_es IS NULL OR omdb_plot_es = '')"

    rows = con.execute(
        f"""
        SELECT id, omdb_plot_en
        FROM movies
        {where}
        ORDER BY id
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
        where = "WHERE extraction_title IS NULL OR extraction_team_json IS NULL OR workflow_needs_review = TRUE"
    elif stage == "imdb":
        where = "WHERE imdb_url IS NULL OR imdb_url = '' OR workflow_needs_review = TRUE"
    elif stage == "omdb":
        where = """
        WHERE imdb_id IS NOT NULL
          AND imdb_id <> ''
          AND ((omdb_status IS NULL OR omdb_status <> 'fetched') OR workflow_needs_review = TRUE)
        """
    elif stage == "translation":
        where = """
        WHERE omdb_plot_en IS NOT NULL
          AND omdb_plot_en <> ''
          AND ((omdb_plot_es IS NULL OR omdb_plot_es = '') OR workflow_needs_review = TRUE)
        """
    else:
        where = "WHERE workflow_status IS NULL OR workflow_status <> 'done'"

    rows = con.execute(
        f"""
        SELECT id
        FROM movies
        {where}
        ORDER BY id
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    con.close()

    return [row[0] for row in rows]
