import hashlib
import importlib
import json
import shutil
import sys
from pathlib import Path

import duckdb
from fastapi.testclient import TestClient

IMPORTAMATIC_EXPORT_COLUMNS = [
    "REFERENCIA",
    "TÍTULO",
    "DESCRIPCIÓN",
    "AUTOR ",
    "PRECIO",
    "OPERACIÓN",
    "SECCIÓN",
    "ESTADO",
    "DESCRIPCIÓN DEL ESTADO",
    "IMAGEN 1 (principal)",
    "IMAGEN 2",
    "IMAGEN 3",
    "FORMA DE ENVÍO",
    "GASTOS FIJOS",
]


def _load_app(
    tmp_path: Path,
    monkeypatch,
    *,
    sync_retention_days: str = "14",
    sync_keep_min: str = "10",
):
    monkeypatch.setenv("PROJECT_ROOT", str(tmp_path))
    monkeypatch.setenv("DB_PATH", str(tmp_path / "movies.duckdb"))
    monkeypatch.setenv("COVERS_DIR", str(tmp_path / "input"))
    monkeypatch.setenv("EXPORTS_DIR", str(tmp_path / "exports"))
    monkeypatch.setenv("TC_SECTIONS_CSV_PATH", str(tmp_path / "secciones.csv"))
    monkeypatch.setenv("BBDD_DIR", str(tmp_path / "bbdd"))
    monkeypatch.setenv("SYNC_STATE_PATH", str(tmp_path / "sync_state.json"))
    monkeypatch.setenv("SYNC_ACTOR", "test-user")
    monkeypatch.setenv("SYNC_DEVICE", "test-device")
    monkeypatch.setenv("SYNC_RETENTION_DAYS", sync_retention_days)
    monkeypatch.setenv("SYNC_KEEP_MIN", sync_keep_min)
    monkeypatch.setenv("IMPORTAMATIC_OTHERS_FIXED_COST", "4.5")
    monkeypatch.setenv("OMDB_PLOT_MODE", "full")
    monkeypatch.setenv("VISION_TITLE_MODEL", "test-title-model")
    monkeypatch.setenv("VISION_TEAM_MODEL", "test-team-model")
    monkeypatch.setenv("TRANSLATION_MODEL", "test-translation-model")
    monkeypatch.setenv("IMDB_MAX_RESULTS", "10")
    monkeypatch.setenv("IMDB_SLEEP_SECONDS", "0")
    monkeypatch.setenv("REQUEST_TIMEOUT_SECONDS", "1")
    monkeypatch.setenv("WORKFLOW_MAX_ATTEMPTS", "2")
    monkeypatch.delenv("OMDB_API_KEY", raising=False)

    for module_name in list(sys.modules):
        if module_name == "src" or module_name.startswith("src."):
            sys.modules.pop(module_name, None)
        if module_name == "backend" or module_name.startswith("backend."):
            sys.modules.pop(module_name, None)

    main = importlib.import_module("src.backend.main")
    return main.app


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def test_versioned_importamatic_template_matches_export_contract():
    template_path = (
        Path(__file__).resolve().parents[1]
        / "data"
        / "plantilla_importamatic_otros.csv"
    )
    header = template_path.read_text(encoding="utf-8-sig").splitlines()[0]
    assert header.split("#") == IMPORTAMATIC_EXPORT_COLUMNS


def test_backend_imports_without_external_api_keys(tmp_path, monkeypatch):
    app = _load_app(tmp_path, monkeypatch)
    client = TestClient(app)

    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
    assert app.version == "0.2.0"


def test_movies_schema_is_initialized_as_normalized_tables(tmp_path, monkeypatch):
    _load_app(tmp_path, monkeypatch)
    db_path = tmp_path / "movies.duckdb"

    with duckdb.connect(str(db_path)) as con:
        tables = {str(row[0]) for row in con.execute("PRAGMA show_tables").fetchall()}
        assert "movies_core" in tables
        assert "movie_extraction" in tables
        assert "movie_imdb" in tables
        assert "movie_omdb" in tables
        assert "movie_workflow" in tables
        assert "movies" in tables
        assert "items" in tables
        assert "inventory_field_allowed_values" in tables
        assert "tc_sections" in tables
        assert "export" in tables
        assert "schema_migrations" in tables
        migrations = con.execute(
            "SELECT version, name FROM schema_migrations ORDER BY version"
        ).fetchall()
        assert migrations == [
            ("0001_baseline", "Registra el esquema actual como baseline")
        ]

        item_columns = {
            str(row[1]): str(row[2])
            for row in con.execute("PRAGMA table_info('items')").fetchall()
        }
        assert "format" not in item_columns
        assert item_columns["tc_section"] == "VARCHAR"
        assert item_columns["listing_status"] == "VARCHAR"
        assert item_columns["stock_status"] == "VARCHAR"

        relation = con.execute(
            """
            SELECT table_type
            FROM information_schema.tables
            WHERE table_schema = current_schema()
              AND table_name = ?
            """,
            ("movies",),
        ).fetchone()
        assert relation is not None
        assert str(relation[0]).upper() == "VIEW"

        export_relation = con.execute("""
            SELECT table_type
            FROM information_schema.tables
            WHERE table_schema = current_schema()
              AND table_name = 'export'
            """).fetchone()
        assert export_relation is not None
        assert str(export_relation[0]).upper() == "VIEW"
        export_cursor = con.execute('SELECT * FROM "export"')
        assert [description[0] for description in export_cursor.description] == (
            IMPORTAMATIC_EXPORT_COLUMNS
        )


def test_item_options_use_cinema_sections_csv(tmp_path, monkeypatch):
    (tmp_path / "secciones.csv").write_text(
        "id sección,título\n" "434,Cine - Películas - DVD\n" "447,Series TV en DVD\n",
        encoding="utf-8",
    )
    app = _load_app(tmp_path, monkeypatch)
    client = TestClient(app)

    response = client.get("/items/options")

    assert response.status_code == 200
    payload = response.json()
    assert payload["allowed_values"] == {
        "listing_status": ["ALTA", "CAMBIO", "BAJA"],
        "stock_status": ["En stock", "Vendido", "Extraviado"],
        "tc_condition": ["5", "4", "3", "2", "1"],
    }
    sections = payload["tc_sections"]
    assert sections["root_key"]
    leaves = {
        str(node["section_id"]): node
        for node in sections["nodes"]
        if node.get("section_id")
    }
    assert leaves["434"]["path_labels"] == [
        "Cine y televisión",
        "Películas",
        "DVD",
    ]
    assert leaves["447"]["path_labels"] == [
        "Cine y televisión",
        "Series TV en DVD",
    ]



def test_cover_read_stores_repo_relative_image_paths(tmp_path, monkeypatch):
    source_dir = tmp_path.parent / f"{tmp_path.name}_covers_source"
    source_dir.mkdir(parents=True)
    (source_dir / "P0001.jpeg").write_bytes(b"caratula-prueba")

    app = _load_app(tmp_path, monkeypatch)
    client = TestClient(app)

    response = client.post(
        "/covers/read",
        json={
            "folder": str(source_dir),
            "recursive": True,
            "extensions": ["jpeg"],
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["inserted"] == 1
    assert payload["copied_to_project"] == 1
    assert (tmp_path / "input" / "P0001.jpeg").read_bytes() == b"caratula-prueba"

    movie_response = client.get("/movies/P0001")
    assert movie_response.status_code == 200
    movie = movie_response.json()
    assert movie["image_path"] == "input/P0001.jpeg"
    assert movie["image_filename"] == "P0001.jpeg"

    (source_dir / "P0001.jpeg").write_bytes(b"caratula-nueva")
    second_response = client.post(
        "/covers/read",
        json={
            "folder": str(source_dir),
            "recursive": True,
            "extensions": ["jpeg"],
            "overwrite_existing_paths": False,
        },
    )
    assert second_response.status_code == 200
    second_payload = second_response.json()
    assert second_payload["skipped"] == 1
    assert second_payload["copied_to_project"] == 0
    assert (tmp_path / "input" / "P0001.jpeg").read_bytes() == b"caratula-prueba"


def test_startup_normalizes_existing_database_image_paths(tmp_path, monkeypatch):
    input_dir = tmp_path / "input"
    input_dir.mkdir(parents=True)
    cover = input_dir / "P0001.jpg"
    cover.write_bytes(b"caratula")

    db_path = tmp_path / "movies.duckdb"
    with duckdb.connect(str(db_path)) as con:
        con.execute("""
            CREATE TABLE movies_core (
                id TEXT PRIMARY KEY,
                image_path TEXT NOT NULL,
                image_filename TEXT,
                created_at TIMESTAMP DEFAULT now(),
                updated_at TIMESTAMP DEFAULT now()
            )
            """)
        con.execute(
            """
            INSERT INTO movies_core (id, image_path, image_filename)
            VALUES ('P0001', ?, 'P0001.jpg')
            """,
            [str(cover.resolve())],
        )

    app = _load_app(tmp_path, monkeypatch)
    client = TestClient(app)

    response = client.get("/movies/P0001")

    assert response.status_code == 200
    assert response.json()["image_path"] == "input/P0001.jpg"


def test_prepare_items_is_idempotent_and_preserves_manual_edits(tmp_path, monkeypatch):
    (tmp_path / "secciones.csv").write_text(
        "id sección,título\n434,Cine - Películas - DVD\n",
        encoding="utf-8",
    )
    app = _load_app(tmp_path, monkeypatch)
    client = TestClient(app)
    db_path = tmp_path / "movies.duckdb"

    with duckdb.connect(str(db_path)) as con:
        con.execute("""
            INSERT INTO movies_core (id, image_path, image_filename)
            VALUES ('P0001', '/tmp/P0001.jpg', 'P0001.jpg')
            """)
        con.execute("""
            INSERT INTO movie_extraction (id, extraction_title, manual_title)
            VALUES ('P0001', 'Título extraído', 'Título manual')
            """)
        con.execute("""
            INSERT INTO movie_imdb (
                id, imdb_url, imdb_title_es, imdb_title_original
            )
            VALUES (
                'P0001', 'https://www.imdb.com/title/tt0000001/',
                'Título IMDb', 'Original Title'
            )
            """)
        con.execute("""
            INSERT INTO movie_omdb (
                id, omdb_title, omdb_year, omdb_rated, omdb_released,
                omdb_runtime, omdb_genre, omdb_director, omdb_writer,
                omdb_actors, omdb_plot_en, omdb_plot_es, omdb_language,
                omdb_country, omdb_awards, omdb_imdbrating, omdb_imdbvotes,
                omdb_boxoffice, omdb_production, omdb_type
            )
            VALUES (
                'P0001', 'OMDb Title', '1999', 'PG-13', '01 Jan 1999',
                '120 min', 'Action, Sci-Fi', 'Directora', 'Guionista',
                'Actriz, Actor', 'English plot', 'Sinopsis en español', 'English, Spanish',
                'United States, Spain', 'Premio', '7.8', '12,345', '$100', 'Productora',
                'movie'
            )
            """)
        con.execute("INSERT INTO movie_workflow (id) VALUES ('P0001')")

    prepared = client.post("/items/prepare")
    assert prepared.status_code == 200
    assert prepared.json() == {"created": 1}

    item_response = client.get("/items/P0001")
    assert item_response.status_code == 200
    item = item_response.json()
    assert item["title"] == "Título IMDb"
    assert item["original_title"] == "Original Title"
    assert item["item_type"] == "Película"
    assert item["director"] == "Directora"
    assert item["genres"] == "Acción, Ciencia ficción"
    assert item["country"] == "Estados Unidos, España"
    assert item["languages"] == "Inglés, Español"
    assert item["plot"] == "Sinopsis en español"
    assert item["listing_status"] == "ALTA"
    assert item["stock_status"] == "En stock"
    assert item["tc_section"] is None

    updated = client.put(
        "/items/P0001",
        json={
            "title": "Título revisado",
            "sale_price": 14.5,
            "listing_status": "CAMBIO",
            "tc_section": 434,
            "tc_condition": "4",
        },
    )
    assert updated.status_code == 200
    assert updated.json()["item"]["tc_section"] == "434"

    prepared_again = client.post("/items/prepare")
    assert prepared_again.status_code == 200
    assert prepared_again.json() == {"created": 0}

    preserved = client.get("/items/P0001").json()
    assert preserved["title"] == "Título revisado"
    assert preserved["sale_price"] == 14.5
    assert preserved["listing_status"] == "CAMBIO"
    assert preserved["tc_condition"] == "4"


def test_importamatic_export_flow_uses_items_and_selected_rows(tmp_path, monkeypatch):
    covers_dir = tmp_path / "input"
    covers_dir.mkdir(parents=True)
    source_cover = covers_dir / "P0001.jpeg"
    source_cover.write_bytes(b"caratula-prueba")

    app = _load_app(tmp_path, monkeypatch)
    client = TestClient(app)
    db_path = tmp_path / "movies.duckdb"
    with duckdb.connect(str(db_path)) as con:
        con.execute(
            """
            INSERT INTO items (
                id, title, original_title, director, writers, actors, year,
                rated, released, runtime, genres, country, languages, plot,
                awards, production, imdb_url, imdb_rating, imdb_votes, box_office,
                sale_price, listing_status, stock_status, tc_section, tc_condition,
                condition_comments, notes, image_path, updated_at
            )
            VALUES (
                'P0001', 'La película', 'The Movie', 'Directora Ejemplo',
                'Guionista Uno', 'Actriz A, Actor B', '1999', 'PG-13',
                '1 de enero de 1999', '120 min', 'Drama, Misterio', 'España',
                'Español, Inglés', 'Sinopsis <especial> & revisada.',
                'Premio importante', 'Productora Uno',
                'https://www.imdb.com/title/tt0000001/', '7.8', '12.345', '100 €',
                19.95, 'ALTA', 'En stock', '434', '4',
                'Buen estado general', 'Primera nota.\n\nSegunda nota.', ?, now()
            )
            """,
            (str(source_cover),),
        )
        con.execute("""
            INSERT INTO items (
                id, title, director, year, sale_price, listing_status,
                stock_status, tc_section, tc_condition, image_path, updated_at
            )
            VALUES (
                'P0002', 'No seleccionada', 'Otro director', '2001', 9.5,
                'CAMBIO', 'En stock', '416', '3', '/tmp/P0002.jpg', now()
            )
            """)

    preview_response = client.get("/export/movies/preview")
    assert preview_response.status_code == 200
    preview = preview_response.json()
    assert preview["columns"] == IMPORTAMATIC_EXPORT_COLUMNS
    assert preview["rows_count"] == 2
    assert set(preview["ids"]) == {"P0001", "P0002"}
    assert preview["validation"]["valid_ids"] == ["P0001"]
    assert preview["validation"]["invalid_ids"] == ["P0002"]

    validation_response = client.post(
        "/export/movies/validate", json={"ids": ["P0001", "P0002"]}
    )
    assert validation_response.status_code == 200
    validation = validation_response.json()
    assert validation["valid_count"] == 1
    assert validation["invalid_count"] == 1

    invalid_export = client.post("/export/movies/csv", json={"ids": ["P0002"]})
    assert invalid_export.status_code == 400
    invalid_detail = invalid_export.json()["detail"]
    assert invalid_detail["invalid_ids"] == ["P0002"]
    assert "Falta una carátula local existente." in invalid_detail["rows"][0]["errors"]

    empty_export = client.post("/export/movies/csv", json={"ids": []})
    assert empty_export.status_code == 200
    assert empty_export.json()["rows"] == 0
    assert empty_export.json()["ids"] == []

    row = next(item for item in preview["rows"] if item["REFERENCIA"] == "P0001")
    assert row["TÍTULO"] == "La película (Directora Ejemplo, 1999)"
    assert row["AUTOR "] == "Directora Ejemplo"
    assert row["PRECIO"] == "19,95"
    assert row["SECCIÓN"] == "434"
    assert row["IMAGEN 1 (principal)"] == "P0001.jpeg"
    assert row["IMAGEN 2"] == "P0001_2.jpg"
    assert row["DESCRIPCIÓN DEL ESTADO"] == "Buen estado general."
    assert "<p><strong>Título original:</strong> The Movie</p>" in row["DESCRIPCIÓN"]
    assert "<p><strong>Guion:</strong> Guionista Uno</p>" in row["DESCRIPCIÓN"]
    assert "Sinopsis &lt;especial&gt; &amp; revisada." in row["DESCRIPCIÓN"]
    assert "Primera nota.<br><br>Segunda nota." in row["DESCRIPCIÓN"]

    export_response = client.post("/export/movies/csv", json={"ids": ["P0001"]})
    assert export_response.status_code == 200
    exported = export_response.json()
    assert exported["rows"] == 1
    assert exported["ids"] == ["P0001"]
    assert exported["filename"].endswith(".csv")

    file_response = client.get(
        "/export/movies/file",
        params={"filename": exported["filename"]},
    )
    assert file_response.status_code == 200
    header, *data_rows = file_response.text.splitlines()
    assert header.split("#") == IMPORTAMATIC_EXPORT_COLUMNS
    assert len(data_rows) == 1
    assert "La película" in data_rows[0]
    assert "No seleccionada" not in file_response.text
    assert "#Otros#4,5" in data_rows[0]

    covers_response = client.post("/export/movies/covers", json={"ids": ["P0001"]})
    assert covers_response.status_code == 200
    covers_payload = covers_response.json()
    assert covers_payload["copied_count"] == 1
    exported_cover = tmp_path / "exports" / "covers" / "P0001.jpeg"
    assert exported_cover.read_bytes() == b"caratula-prueba"

    clear_response = client.post(
        "/export/movies/clear-operation", json={"ids": ["P0001"]}
    )
    assert clear_response.status_code == 200
    assert clear_response.json()["updated"] == 1
    with duckdb.connect(str(db_path)) as con:
        statuses = dict(
            con.execute(
                "SELECT id, listing_status FROM items WHERE id IN ('P0001', 'P0002')"
            ).fetchall()
        )
    assert statuses["P0001"] is None
    assert statuses["P0002"] == "CAMBIO"


def test_omdb_cover_download_uses_second_image_convention(tmp_path, monkeypatch):
    app = _load_app(tmp_path, monkeypatch)
    client = TestClient(app)
    db_path = tmp_path / "movies.duckdb"

    with duckdb.connect(str(db_path)) as con:
        con.execute(
            """
            INSERT INTO movies_core (id, image_path, image_filename)
            VALUES ('P0001', '/tmp/P0001.jpg', 'P0001.jpg')
            """
        )
        con.execute(
            """
            INSERT INTO movie_omdb (id, omdb_poster)
            VALUES ('P0001', 'N/A; https://img.example.test/poster.jpg')
            """
        )

    calls = []

    class FakeResponse:
        headers = {"Content-Type": "image/jpeg"}
        content = b"poster-omdb"

        def raise_for_status(self):
            return None

    def fake_get(url, *, timeout, headers):
        calls.append({"url": url, "timeout": timeout, "headers": headers})
        return FakeResponse()

    from src.backend.services import export as export_service

    monkeypatch.setattr(export_service.requests, "get", fake_get)

    response = client.post(
        "/omdb/covers/download",
        json={"movie_id": "P0001", "poster_slot": 2},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["downloaded_count"] == 1
    assert payload["skipped_count"] == 0
    assert payload["failed_count"] == 0
    assert payload["output_dir"] == str(tmp_path / "data" / "output" / "covers")
    assert calls[0]["url"] == "https://img.example.test/poster.jpg"

    output_cover = tmp_path / "data" / "output" / "covers" / "P0001_2.jpg"
    assert output_cover.read_bytes() == b"poster-omdb"


def test_snapshots_publish_and_list(tmp_path, monkeypatch):
    app = _load_app(tmp_path, monkeypatch)
    client = TestClient(app)

    status_response = client.get("/snapshots/status")
    assert status_response.status_code == 200
    status = status_response.json()
    assert status["local_db_exists"] is True
    assert status["bbdd_root"] == str(tmp_path / "bbdd")
    assert status["cloud_root"] == str(tmp_path / "bbdd" / "media-catalog-movies")
    assert status["snapshots_count"] == 0

    publish_response = client.post(
        "/snapshots/publish",
        json={"notes": "Snapshot de prueba", "cleanup": True},
    )
    assert publish_response.status_code == 200
    payload = publish_response.json()
    snapshot = payload["snapshot"]
    assert snapshot["valid"] is True
    assert snapshot["notes"] == "Snapshot de prueba"
    assert snapshot["source_actor"] == "test-user"
    assert snapshot["source_device"] == "test-device"

    db_path = Path(snapshot["path"])
    manifest_path = Path(snapshot["manifest_path"])
    assert db_path.exists()
    assert manifest_path.exists()

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["sha256"] == snapshot["sha256"]
    assert manifest["db_filename"] == db_path.name

    with duckdb.connect(str(db_path)) as con:
        tables = {str(row[0]) for row in con.execute("PRAGMA show_tables").fetchall()}
    assert "items" in tables
    assert "movies_core" in tables

    list_response = client.get("/snapshots")
    assert list_response.status_code == 200
    snapshots = list_response.json()["snapshots"]
    assert len(snapshots) == 1
    assert snapshots[0]["snapshot_id"] == snapshot["snapshot_id"]

    state = json.loads((tmp_path / "sync_state.json").read_text(encoding="utf-8"))
    assert state["last_published_snapshot_id"] == snapshot["snapshot_id"]


def test_snapshots_detects_and_imports_external_snapshot(tmp_path, monkeypatch):
    app = _load_app(tmp_path, monkeypatch)
    client = TestClient(app)

    publish_response = client.post(
        "/snapshots/publish",
        json={"notes": "Snapshot base", "cleanup": False},
    )
    assert publish_response.status_code == 200
    snapshot = publish_response.json()["snapshot"]

    snapshots_dir = Path(snapshot["manifest_path"]).parent
    external_id = "29990101_010101_000000_dani_laptop"
    external_db_path = snapshots_dir / f"{external_id}.duckdb"
    shutil.copy2(Path(snapshot["path"]), external_db_path)

    external_manifest = json.loads(
        Path(snapshot["manifest_path"]).read_text(encoding="utf-8")
    )
    external_manifest.update(
        {
            "snapshot_id": external_id,
            "created_at": "2999-01-01T01:01:01+00:00",
            "source_actor": "dani",
            "source_device": "laptop",
            "db_filename": external_db_path.name,
            "sha256": _sha256_file(external_db_path),
            "notes": "Snapshot externo",
        }
    )
    (snapshots_dir / f"{external_id}.json").write_text(
        json.dumps(external_manifest, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )

    with duckdb.connect(str(tmp_path / "movies.duckdb")) as con:
        con.execute(
            "INSERT INTO items (id, title, listing_status, updated_at) VALUES (?, ?, ?, now())",
            ["PLOCAL", "Edición local sin publicar", "ALTA"],
        )

    status_response = client.get("/snapshots/status")
    assert status_response.status_code == 200
    status = status_response.json()
    assert status["has_external_snapshot"] is True
    assert status["latest_external_snapshot"]["snapshot_id"] == external_id

    unconfirmed_response = client.post(
        "/snapshots/import", json={"snapshot_id": external_id, "confirm": False}
    )
    assert unconfirmed_response.status_code == 400

    import_response = client.post(
        "/snapshots/import", json={"snapshot_id": external_id, "confirm": True}
    )
    assert import_response.status_code == 200
    import_payload = import_response.json()
    assert import_payload["snapshot"]["snapshot_id"] == external_id

    backup_path = Path(import_payload["backup_path"])
    assert backup_path.exists()

    with duckdb.connect(str(backup_path)) as con:
        backup_count = con.execute(
            "SELECT count(*) FROM items WHERE id = ?", ["PLOCAL"]
        ).fetchone()[0]
    assert backup_count == 1

    with duckdb.connect(str(tmp_path / "movies.duckdb")) as con:
        local_count = con.execute(
            "SELECT count(*) FROM items WHERE id = ?", ["PLOCAL"]
        ).fetchone()[0]
    assert local_count == 0

    state = json.loads((tmp_path / "sync_state.json").read_text(encoding="utf-8"))
    assert state["last_imported_snapshot_id"] == external_id
    assert state["last_import_backup_path"] == str(backup_path)

    refreshed_status = client.get("/snapshots/status")
    assert refreshed_status.status_code == 200
    assert refreshed_status.json()["has_external_snapshot"] is False


def test_snapshot_cleanup_keeps_configured_minimum(tmp_path, monkeypatch):
    app = _load_app(
        tmp_path,
        monkeypatch,
        sync_retention_days="0",
        sync_keep_min="2",
    )
    client = TestClient(app)

    published_paths = []
    for index in range(3):
        response = client.post(
            "/snapshots/publish",
            json={"notes": f"Snapshot {index}", "cleanup": False},
        )
        assert response.status_code == 200
        published_paths.append(Path(response.json()["snapshot"]["path"]))

    cleanup_response = client.post("/snapshots/cleanup")
    assert cleanup_response.status_code == 200
    cleanup_payload = cleanup_response.json()
    assert len(cleanup_payload["deleted"]) == 1
    assert len(cleanup_payload["kept"]) == 2

    list_response = client.get("/snapshots")
    assert list_response.status_code == 200
    assert len(list_response.json()["snapshots"]) == 2
    assert sum(1 for path in published_paths if path.exists()) == 2

def test_snapshots_report_incomplete_and_corrupt_manifests(tmp_path, monkeypatch):
    app = _load_app(tmp_path, monkeypatch)
    client = TestClient(app)

    publish_response = client.post(
        "/snapshots/publish",
        json={"notes": "Snapshot base", "cleanup": False},
    )
    assert publish_response.status_code == 200
    snapshot = publish_response.json()["snapshot"]
    snapshots_dir = Path(snapshot["manifest_path"]).parent
    manifest_template = json.loads(
        Path(snapshot["manifest_path"]).read_text(encoding="utf-8")
    )

    missing_id = "29990101_020202_000000_dani_missing"
    missing_manifest = dict(manifest_template)
    missing_manifest.update(
        {
            "snapshot_id": missing_id,
            "created_at": "2999-01-01T02:02:02+00:00",
            "source_actor": "dani",
            "source_device": "missing",
            "db_filename": f"{missing_id}.duckdb",
            "sha256": "missing",
        }
    )
    (snapshots_dir / f"{missing_id}.json").write_text(
        json.dumps(missing_manifest, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )

    corrupt_id = "29990101_030303_000000_dani_corrupt"
    corrupt_db_path = snapshots_dir / f"{corrupt_id}.duckdb"
    shutil.copy2(Path(snapshot["path"]), corrupt_db_path)
    corrupt_db_path.write_bytes(corrupt_db_path.read_bytes() + b"corrupt")
    corrupt_manifest = dict(manifest_template)
    corrupt_manifest.update(
        {
            "snapshot_id": corrupt_id,
            "created_at": "2999-01-01T03:03:03+00:00",
            "source_actor": "dani",
            "source_device": "corrupt",
            "db_filename": corrupt_db_path.name,
            "sha256": snapshot["sha256"],
        }
    )
    (snapshots_dir / f"{corrupt_id}.json").write_text(
        json.dumps(corrupt_manifest, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )

    list_response = client.get("/snapshots")
    assert list_response.status_code == 200
    snapshots = {
        item["snapshot_id"]: item for item in list_response.json()["snapshots"]
    }
    assert snapshots[missing_id]["valid"] is False
    assert snapshots[missing_id]["error"] == "El fichero DuckDB del snapshot no existe."
    assert snapshots[corrupt_id]["valid"] is False
    assert snapshots[corrupt_id]["error"] == "El hash sha256 no coincide."

    import_response = client.post(
        "/snapshots/import", json={"snapshot_id": corrupt_id, "confirm": True}
    )
    assert import_response.status_code == 400

