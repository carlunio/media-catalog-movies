from __future__ import annotations

import argparse
import json

from src.backend.services import migrations


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Gestiona las migraciones de esquema de la base DuckDB."
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Muestra el estado sin aplicar migraciones pendientes.",
    )
    args = parser.parse_args()

    data = migrations.get_status() if args.status else None
    if data is None:
        data = migrations.migrate()

    print(json.dumps(data, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
