from __future__ import annotations

import argparse
from pathlib import Path

import duckdb


def _file_size_mb(path: Path) -> float:
    if not path.exists():
        return 0.0
    return path.stat().st_size / (1024 * 1024)


def _duckdb_sql_string(path: Path) -> str:
    return "'" + path.as_posix().replace("'", "''") + "'"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Mantenimiento DuckDB para media-catalog-movies"
    )
    parser.add_argument(
        "--db",
        default="data/movies.duckdb",
        help="Ruta del fichero DuckDB (por defecto: data/movies.duckdb)",
    )
    parser.add_argument(
        "--repack",
        action="store_true",
        help="Crea una copia compactada como <db>.repacked.duckdb",
    )
    parser.add_argument(
        "--replace",
        action="store_true",
        help="Sustituye la DB original por la copia compactada; requiere --repack",
    )
    args = parser.parse_args()
    if args.replace and not args.repack:
        raise SystemExit("--replace requiere --repack")

    db_path = Path(args.db).expanduser().resolve()
    if not db_path.exists():
        raise SystemExit(f"No existe la base de datos: {db_path}")

    before_mb = _file_size_mb(db_path)
    print(f"DB: {db_path}")
    print(f"Tamaño antes: {before_mb:.2f} MB")

    with duckdb.connect(str(db_path)) as con:
        con.execute("CHECKPOINT")
        con.execute("VACUUM")

        try:
            info = con.execute("PRAGMA database_size").fetchall()
            if info:
                print(f"PRAGMA database_size: {info[0]}")
        except Exception:
            pass

    after_mb = _file_size_mb(db_path)
    delta_mb = after_mb - before_mb
    print(f"Tamaño después: {after_mb:.2f} MB")
    print(f"Diferencia: {delta_mb:+.2f} MB")

    repacked_path: Path | None = None
    if args.repack:
        repacked_path = db_path.with_suffix(".repacked.duckdb")
        if repacked_path.exists():
            repacked_path.unlink()

        with duckdb.connect(str(db_path)) as con:
            db_list = con.execute("PRAGMA database_list").fetchall()
            if not db_list:
                raise SystemExit("No se pudo resolver el catálogo activo de DuckDB")
            catalog_name = str(db_list[0][1])
            con.execute(f"ATTACH {_duckdb_sql_string(repacked_path)} AS repacked")
            con.execute(f'COPY FROM DATABASE "{catalog_name}" TO repacked')
            con.execute("DETACH repacked")

        repacked_mb = _file_size_mb(repacked_path)
        print(f"Copia compactada: {repacked_path}")
        print(f"Tamaño compactado: {repacked_mb:.2f} MB")

    if args.replace and repacked_path is not None:
        if not repacked_path.exists():
            raise SystemExit(f"No existe la copia compactada: {repacked_path}")
        backup_path = db_path.with_suffix(".pre_repack.bak.duckdb")
        if backup_path.exists():
            backup_path.unlink()
        db_path.replace(backup_path)
        repacked_path.replace(db_path)
        final_mb = _file_size_mb(db_path)
        print(f"DB original movida a backup: {backup_path}")
        print(f"Sustitución completada. Nuevo tamaño: {final_mb:.2f} MB")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
