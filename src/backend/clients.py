import time
from typing import Any

import requests

from .config import REQUEST_TIMEOUT_SECONDS

try:
    import ollama  # type: ignore
except Exception:  # pragma: no cover
    ollama = None


class ClientError(Exception):
    pass


def http_get_json(url: str, *, params: dict[str, Any] | None = None) -> dict[str, Any]:
    response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()
    data = response.json()
    if not isinstance(data, dict):
        raise ClientError("Invalid JSON payload")
    return data


def ollama_chat(
    *,
    model: str,
    messages: list[dict[str, Any]],
    sleep_seconds: float = 0.0,
) -> str:
    if ollama is None:
        raise ClientError(
            "ollama package is not available. Install dependencies in the project virtualenv."
        )

    try:
        response = ollama.chat(model=model, messages=messages)
    except Exception as exc:  # pragma: no cover
        raise ClientError(str(exc)) from exc

    content = response.get("message", {}).get("content", "")
    if sleep_seconds > 0:
        time.sleep(sleep_seconds)
    return str(content).strip()
