from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

TC_SECTION_ROOT_LABEL = "Cine y televisión"


def normalize_tc_section_value(value: Any) -> str | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return str(int(value)) if value.is_integer() else None
    text = str(value).strip()
    if not text or text.lower() in {"none", "null", "nan"}:
        return None
    return text


def _tc_node_key(path_labels: list[str] | tuple[str, ...]) -> str:
    return json.dumps(list(path_labels), ensure_ascii=False)


def _section_path_labels(title: str) -> list[str]:
    raw_parts = [part.strip() for part in str(title or "").split(" - ") if part.strip()]
    if not raw_parts:
        return []
    if raw_parts[0].casefold() == "cine":
        raw_parts = raw_parts[1:]
    return [TC_SECTION_ROOT_LABEL, *raw_parts]


def build_tc_section_nodes(csv_path: Path) -> list[dict[str, Any]]:
    if not csv_path.exists():
        return []

    nodes_by_key: dict[str, dict[str, Any]] = {}
    next_sort_order_by_parent: dict[str | None, int] = {}
    with csv_path.open("r", encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(file)
        for row in reader:
            section_id = normalize_tc_section_value(row.get("id sección"))
            path_labels = _section_path_labels(str(row.get("título") or ""))
            if section_id is None or not path_labels:
                continue

            path_keys: list[str] = []
            for depth in range(1, len(path_labels) + 1):
                node_path = path_labels[:depth]
                node_key = _tc_node_key(node_path)
                path_keys.append(node_key)
                if node_key in nodes_by_key:
                    continue
                parent_key = path_keys[-2] if len(path_keys) > 1 else None
                sort_order = next_sort_order_by_parent.get(parent_key, 0)
                next_sort_order_by_parent[parent_key] = sort_order + 1
                nodes_by_key[node_key] = {
                    "node_key": node_key,
                    "parent_key": parent_key,
                    "section_id": None,
                    "label": path_labels[depth - 1],
                    "depth": depth - 1,
                    "path_labels": list(node_path),
                    "path_keys": list(path_keys),
                    "path_text": " > ".join(node_path),
                    "display_path": " > ".join(node_path[1:]) or node_path[0],
                    "is_leaf": False,
                    "sort_order": sort_order,
                }

            leaf = nodes_by_key[path_keys[-1]]
            leaf["section_id"] = section_id
            leaf["is_leaf"] = True

    return sorted(
        nodes_by_key.values(),
        key=lambda node: (
            int(node["depth"]),
            int(node["sort_order"]),
            str(node["path_text"]),
        ),
    )
