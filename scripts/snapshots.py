from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.backend.services import snapshots


def _print_json(data: dict) -> None:
    print(json.dumps(data, indent=2, ensure_ascii=False))


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Gestión de snapshots para media-catalog-movies"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("status", help="Muestra la configuración y el estado")
    subparsers.add_parser("list", help="Lista los snapshots disponibles")

    publish_parser = subparsers.add_parser("publish", help="Publica un snapshot")
    publish_parser.add_argument(
        "--notes", default=None, help="Notas opcionales para el manifiesto"
    )
    publish_parser.add_argument(
        "--no-cleanup", action="store_true", help="No limpiar después de publicar"
    )

    import_parser = subparsers.add_parser(
        "import", help="Importa un snapshot verificado en la DB local"
    )
    import_parser.add_argument("snapshot_id", help="ID del snapshot que se importará")
    import_parser.add_argument(
        "--confirm", action="store_true", help="Confirma la sustitución de la DB local"
    )

    subparsers.add_parser("cleanup", help="Elimina snapshots antiguos según retención")

    args = parser.parse_args()
    if args.command == "status":
        _print_json(snapshots.get_status())
    elif args.command == "list":
        _print_json({"ok": True, "snapshots": snapshots.list_snapshots()})
    elif args.command == "publish":
        _print_json(
            snapshots.publish_snapshot(notes=args.notes, cleanup=not args.no_cleanup)
        )
    elif args.command == "import":
        _print_json(
            snapshots.import_snapshot(
                snapshot_id=args.snapshot_id, confirm=args.confirm
            )
        )
    elif args.command == "cleanup":
        _print_json({"ok": True, **snapshots.cleanup_snapshots()})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
