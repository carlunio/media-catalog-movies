import os
from pathlib import Path


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


PROJECT_ROOT = Path(os.getenv("PROJECT_ROOT", Path.cwd())).resolve()


def _resolve_path(env_name: str, default_relative: str) -> Path:
    raw = os.getenv(env_name, default_relative)
    path = Path(raw)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path


DB_PATH = _resolve_path("DB_PATH", "data/movies.duckdb")
DEFAULT_COVERS_DIR = _resolve_path("COVERS_DIR", "data/input")

OMDB_API_KEY = os.getenv("OMDB_API_KEY")
VISION_TITLE_MODEL = os.getenv("VISION_TITLE_MODEL", "gemma3:27b-it-qat")
VISION_TEAM_MODEL = os.getenv("VISION_TEAM_MODEL", "qwen3-vl:32b")
TRANSLATION_MODEL = os.getenv("TRANSLATION_MODEL", "phi4:latest")

IMDB_MAX_RESULTS = _as_int(os.getenv("IMDB_MAX_RESULTS", "10"), 10)
IMDB_SLEEP_SECONDS = _as_float(os.getenv("IMDB_SLEEP_SECONDS", "1.0"), 1.0)
REQUEST_TIMEOUT_SECONDS = _as_float(os.getenv("REQUEST_TIMEOUT_SECONDS", "20"), 20.0)


if __name__ == "__main__":
    print("PROJECT_ROOT:", PROJECT_ROOT)
    print("DB_PATH:", DB_PATH)
    print("DEFAULT_COVERS_DIR:", DEFAULT_COVERS_DIR)
    print("OMDB_API_KEY:", "OK" if OMDB_API_KEY else "MISSING")
    print("VISION_TITLE_MODEL:", VISION_TITLE_MODEL)
    print("VISION_TEAM_MODEL:", VISION_TEAM_MODEL)
    print("TRANSLATION_MODEL:", TRANSLATION_MODEL)
