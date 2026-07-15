import importlib
import sys
from pathlib import Path

import duckdb


def _reload_migrations(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("PROJECT_ROOT", str(tmp_path))
    monkeypatch.setenv("DB_PATH", str(tmp_path / "movies.duckdb"))
    monkeypatch.setenv("COVERS_DIR", str(tmp_path / "covers"))
    monkeypatch.setenv("EXPORTS_DIR", str(tmp_path / "exports"))
    monkeypatch.setenv("TC_SECTIONS_CSV_PATH", str(tmp_path / "secciones.csv"))
    monkeypatch.setenv("BBDD_DIR", str(tmp_path / "bbdd"))
    monkeypatch.setenv("SYNC_STATE_PATH", str(tmp_path / "sync_state.json"))
    monkeypatch.setenv("SYNC_ACTOR", "test-user")
    monkeypatch.setenv("SYNC_DEVICE", "test-device")
    monkeypatch.setenv("SYNC_RETENTION_DAYS", "14")
    monkeypatch.setenv("SYNC_KEEP_MIN", "10")
    monkeypatch.setenv("IMPORTAMATIC_OTHERS_FIXED_COST", "4.5")

    for module_name in list(sys.modules):
        if module_name == "src" or module_name.startswith("src."):
            sys.modules.pop(module_name, None)

    return importlib.import_module("src.backend.services.migrations")


def test_migrate_creates_initial_schema_without_importing_main(tmp_path, monkeypatch):
    (tmp_path / "secciones.csv").write_text(
        "id sección,título\n434,Cine - Películas - DVD\n",
        encoding="utf-8",
    )
    migrations = _reload_migrations(tmp_path, monkeypatch)

    first_status = migrations.migrate()
    second_status = migrations.migrate()

    assert first_status["applied_now"] == ["0001_baseline"]
    assert first_status["pending_count"] == 0
    assert second_status["applied_now"] == []
    assert second_status["pending_count"] == 0

    with duckdb.connect(str(tmp_path / "movies.duckdb")) as con:
        tables = {str(row[0]) for row in con.execute("PRAGMA show_tables").fetchall()}
        assert {
            "movies_core",
            "movie_extraction",
            "movie_imdb",
            "movie_omdb",
            "movie_workflow",
            "items",
            "tc_sections",
            "export",
            "schema_migrations",
        }.issubset(tables)

        applied = con.execute(
            "SELECT version, name FROM schema_migrations ORDER BY version"
        ).fetchall()
        assert applied == [
            ("0001_baseline", "Registra el esquema actual como baseline")
        ]
