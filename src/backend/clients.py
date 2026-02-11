import time
import subprocess
from typing import Any

import requests

from .config import REQUEST_TIMEOUT_SECONDS

try:
    import ollama  # type: ignore
except Exception:  # pragma: no cover
    ollama = None


class ClientError(Exception):
    pass


def _dedupe_keep_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        if value not in seen:
            ordered.append(value)
            seen.add(value)
    return ordered


def _parse_ollama_list_output(text: str) -> list[str]:
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if not lines:
        return []

    if lines[0].lower().startswith("name"):
        lines = lines[1:]

    models: list[str] = []
    for line in lines:
        model = line.split()[0].strip()
        if model:
            models.append(model)

    return _dedupe_keep_order(models)


def list_ollama_models() -> list[str]:
    errors: list[str] = []

    if ollama is not None:
        try:
            payload = ollama.list()
            raw_models = payload.get("models", []) if isinstance(payload, dict) else []
            parsed: list[str] = []
            for item in raw_models:
                if isinstance(item, dict):
                    model = str(item.get("model") or item.get("name") or "").strip()
                else:
                    model = str(item).strip()
                if model:
                    parsed.append(model)

            parsed = _dedupe_keep_order(parsed)
            if parsed:
                return parsed
            errors.append("No models returned by ollama Python client")
        except Exception as exc:  # pragma: no cover
            errors.append(str(exc))
    else:
        errors.append("ollama package is not available")

    try:
        completed = subprocess.run(
            ["ollama", "list"],
            capture_output=True,
            text=True,
            check=True,
            timeout=max(5.0, REQUEST_TIMEOUT_SECONDS),
        )
        parsed = _parse_ollama_list_output(completed.stdout)
        if parsed:
            return parsed
        errors.append("No models returned by `ollama list`")
    except Exception as exc:  # pragma: no cover
        errors.append(str(exc))

    raise ClientError("Unable to list Ollama models: " + " ; ".join(errors))


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
