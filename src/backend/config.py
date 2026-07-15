import os
import socket
from decimal import Decimal, InvalidOperation
from pathlib import Path

SNAPSHOTS_REPO_DIRNAME = "media-catalog-movies"


def _as_float(value: str, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _as_int(value: str, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _parse_int_setting(env_name: str, default_value: str, *, minimum: int) -> int:
    raw_value = str(os.getenv(env_name, default_value)).strip()
    try:
        parsed_value = int(raw_value)
    except ValueError as exc:
        raise ValueError(
            f"{env_name} debe ser un entero; valor recibido: {raw_value!r}"
        ) from exc
    if parsed_value < minimum:
        raise ValueError(
            f"{env_name} debe ser al menos {minimum}; valor recibido: {raw_value!r}"
        )
    return parsed_value


def _parse_decimal_setting(env_name: str, default_value: str) -> Decimal:
    raw_value = str(os.getenv(env_name, default_value)).strip()
    try:
        value = Decimal(raw_value.replace(",", "."))
    except InvalidOperation as exc:
        raise ValueError(
            f"{env_name} debe ser un número decimal; valor recibido: {raw_value!r}"
        ) from exc
    if value < 0:
        raise ValueError(f"{env_name} debe ser cero o positivo")
    return value


def _format_decimal_for_export(value: Decimal) -> str:
    text = format(value.normalize(), "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return (text or "0").replace(".", ",")


PROJECT_ROOT = Path(os.getenv("PROJECT_ROOT", Path.cwd())).resolve()


def _resolve_path(env_name: str, default_relative: str) -> Path:
    raw = os.getenv(env_name, default_relative)
    path = Path(raw)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path


DB_PATH = _resolve_path("DB_PATH", "data/movies.duckdb")
DEFAULT_COVERS_DIR = _resolve_path("COVERS_DIR", "data/input")
EXPORTS_DIR = _resolve_path("EXPORTS_DIR", "data/exports")
TC_SECTIONS_CSV_PATH = _resolve_path("TC_SECTIONS_CSV_PATH", "data/secciones.csv")
BBDD_DIR = _resolve_path("BBDD_DIR", "../bbdd")
CLOUD_SNAPSHOTS_DIR = BBDD_DIR / SNAPSHOTS_REPO_DIRNAME
SYNC_STATE_PATH = _resolve_path("SYNC_STATE_PATH", "data/sync_state.json")
SYNC_ACTOR = os.getenv("SYNC_ACTOR", os.getenv("USER", "usuario")).strip() or "usuario"
SYNC_DEVICE = os.getenv("SYNC_DEVICE", socket.gethostname()).strip() or "equipo"
SYNC_RETENTION_DAYS = _parse_int_setting("SYNC_RETENTION_DAYS", "14", minimum=0)
SYNC_KEEP_MIN = _parse_int_setting("SYNC_KEEP_MIN", "10", minimum=1)
IMPORTAMATIC_OTHERS_FIXED_COST = _parse_decimal_setting(
    "IMPORTAMATIC_OTHERS_FIXED_COST", "4.5"
)
IMPORTAMATIC_OTHERS_FIXED_COST_EXPORT = _format_decimal_for_export(
    IMPORTAMATIC_OTHERS_FIXED_COST
)

OMDB_API_KEY = os.getenv("OMDB_API_KEY")
OMDB_PLOT_MODE = os.getenv("OMDB_PLOT_MODE", "full").strip().lower() or "full"
VISION_TITLE_MODEL = os.getenv("VISION_TITLE_MODEL", "gemma3:27b-it-qat")
VISION_TEAM_MODEL = os.getenv("VISION_TEAM_MODEL", "qwen3-vl:32b")
TRANSLATION_MODEL = os.getenv("TRANSLATION_MODEL", "phi4:latest")

IMDB_MAX_RESULTS = _as_int(os.getenv("IMDB_MAX_RESULTS", "10"), 10)
IMDB_SLEEP_SECONDS = _as_float(os.getenv("IMDB_SLEEP_SECONDS", "1.0"), 1.0)
REQUEST_TIMEOUT_SECONDS = _as_float(os.getenv("REQUEST_TIMEOUT_SECONDS", "20"), 20.0)
WORKFLOW_MAX_ATTEMPTS = _as_int(os.getenv("WORKFLOW_MAX_ATTEMPTS", "2"), 2)


if __name__ == "__main__":
    print("PROJECT_ROOT:", PROJECT_ROOT)
    print("DB_PATH:", DB_PATH)
    print("DEFAULT_COVERS_DIR:", DEFAULT_COVERS_DIR)
    print("EXPORTS_DIR:", EXPORTS_DIR)
    print("TC_SECTIONS_CSV_PATH:", TC_SECTIONS_CSV_PATH)
    print("BBDD_DIR:", BBDD_DIR)
    print("CLOUD_SNAPSHOTS_DIR:", CLOUD_SNAPSHOTS_DIR)
    print("SYNC_STATE_PATH:", SYNC_STATE_PATH)
    print("SYNC_ACTOR:", SYNC_ACTOR)
    print("SYNC_DEVICE:", SYNC_DEVICE)
    print("SYNC_RETENTION_DAYS:", SYNC_RETENTION_DAYS)
    print("SYNC_KEEP_MIN:", SYNC_KEEP_MIN)
    print("IMPORTAMATIC_OTHERS_FIXED_COST:", IMPORTAMATIC_OTHERS_FIXED_COST_EXPORT)
    print("OMDB_API_KEY:", "OK" if OMDB_API_KEY else "MISSING")
    print("OMDB_PLOT_MODE:", OMDB_PLOT_MODE)
    print("VISION_TITLE_MODEL:", VISION_TITLE_MODEL)
    print("VISION_TEAM_MODEL:", VISION_TEAM_MODEL)
    print("TRANSLATION_MODEL:", TRANSLATION_MODEL)
    print("WORKFLOW_MAX_ATTEMPTS:", WORKFLOW_MAX_ATTEMPTS)
