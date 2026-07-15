from __future__ import annotations

from typing import Any

import streamlit as st

TC_SECTION_MAX_LEVELS = 8


def display_text(value: Any) -> str:
    text = str(value or "").strip()
    if text.lower() in {"none", "null", "nan"}:
        return ""
    return text


def normalize_tc_section_value(value: Any) -> str | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return str(int(value)) if value.is_integer() else None
    return display_text(value) or None


def build_tc_sections_index(payload: Any) -> dict[str, Any]:
    nodes_payload = payload.get("nodes") if isinstance(payload, dict) else []
    nodes = []
    for raw_node in nodes_payload if isinstance(nodes_payload, list) else []:
        if not isinstance(raw_node, dict):
            continue

        node_key = display_text(raw_node.get("node_key"))
        label = display_text(raw_node.get("label"))
        if not node_key or not label:
            continue

        path_labels = [
            str(item).strip()
            for item in raw_node.get("path_labels") or []
            if str(item).strip()
        ]
        path_keys = [
            str(item).strip()
            for item in raw_node.get("path_keys") or []
            if str(item).strip()
        ]
        try:
            depth = int(raw_node.get("depth") or 0)
        except (TypeError, ValueError):
            depth = 0
        try:
            sort_order = int(raw_node.get("sort_order") or 0)
        except (TypeError, ValueError):
            sort_order = 0

        nodes.append(
            {
                "node_key": node_key,
                "parent_key": display_text(raw_node.get("parent_key")) or None,
                "section_id": normalize_tc_section_value(raw_node.get("section_id")),
                "label": label,
                "depth": depth,
                "path_labels": path_labels,
                "path_keys": path_keys,
                "path_text": display_text(raw_node.get("path_text"))
                or " > ".join(path_labels),
                "display_path": display_text(raw_node.get("display_path"))
                or " > ".join(
                    path_labels[1:] if len(path_labels) > 1 else path_labels
                ),
                "is_leaf": bool(raw_node.get("is_leaf")),
                "sort_order": sort_order,
            }
        )

    nodes.sort(key=lambda node: (node["depth"], node["sort_order"], node["display_path"]))
    children_by_parent: dict[str | None, list[dict[str, Any]]] = {}
    leaf_by_section_id: dict[str, dict[str, Any]] = {}
    nodes_by_key = {node["node_key"]: node for node in nodes}
    for node in nodes:
        children_by_parent.setdefault(node["parent_key"], []).append(node)
        if node["is_leaf"] and node["section_id"] is not None:
            leaf_by_section_id[node["section_id"]] = node

    root_key = display_text(payload.get("root_key")) if isinstance(payload, dict) else ""
    if not root_key:
        root_key = next((node["node_key"] for node in nodes if node["depth"] == 0), "")

    return {
        "nodes": nodes,
        "nodes_by_key": nodes_by_key,
        "children_by_parent": children_by_parent,
        "leaf_by_section_id": leaf_by_section_id,
        "root_key": root_key or None,
    }


def _state_key(record_id: str, suffix: str, *, state_key_prefix: str) -> str:
    return f"{state_key_prefix}_{record_id}_tc_section_{suffix}"


def sync_tc_section_state(
    record_id: str,
    current_section_id: Any,
    sections_index: dict[str, Any],
    *,
    state_key_prefix: str,
) -> None:
    keys = {
        "value": _state_key(record_id, "value", state_key_prefix=state_key_prefix),
        "path": _state_key(record_id, "path", state_key_prefix=state_key_prefix),
        "source": _state_key(record_id, "source", state_key_prefix=state_key_prefix),
    }
    normalized_current = normalize_tc_section_value(current_section_id)
    if st.session_state.get(keys["source"]) == normalized_current:
        return

    leaf_node = sections_index["leaf_by_section_id"].get(normalized_current)
    st.session_state[keys["value"]] = normalized_current if leaf_node else None
    st.session_state[keys["path"]] = list((leaf_node or {}).get("path_keys") or [])[1:]
    st.session_state[keys["source"]] = normalized_current


def get_tc_section_value(record_id: str, *, state_key_prefix: str) -> str | None:
    return normalize_tc_section_value(
        st.session_state.get(
            _state_key(record_id, "value", state_key_prefix=state_key_prefix)
        )
    )


def _sync_tc_section_from_pickers(
    record_id: str,
    sections_index: dict[str, Any],
    *,
    state_key_prefix: str,
) -> None:
    path_state_key = _state_key(record_id, "path", state_key_prefix=state_key_prefix)
    value_state_key = _state_key(record_id, "value", state_key_prefix=state_key_prefix)
    parent_key = sections_index["root_key"]
    new_path = []
    selected_leaf_id = None

    for level in range(TC_SECTION_MAX_LEVELS):
        children = sections_index["children_by_parent"].get(parent_key, [])
        if not children:
            break

        widget_key = _state_key(
            record_id, f"picker_{level}", state_key_prefix=state_key_prefix
        )
        if widget_key not in st.session_state:
            break

        choice = display_text(st.session_state.get(widget_key))
        if not choice:
            break

        valid_choices = {child["node_key"] for child in children}
        if choice not in valid_choices:
            break

        new_path.append(choice)
        node = sections_index["nodes_by_key"][choice]
        parent_key = choice
        if node["is_leaf"]:
            selected_leaf_id = node["section_id"]
            break

    st.session_state[path_state_key] = new_path
    st.session_state[value_state_key] = selected_leaf_id

    for stale_level in range(max(len(new_path), 1), TC_SECTION_MAX_LEVELS):
        stale_key = _state_key(
            record_id, f"picker_{stale_level}", state_key_prefix=state_key_prefix
        )
        st.session_state.pop(stale_key, None)


def render_tc_section_selector(
    record_id: str,
    sections_index: dict[str, Any],
    *,
    state_key_prefix: str,
    container=st,
) -> None:
    if not sections_index["nodes"] or not sections_index["root_key"]:
        container.warning("No se han podido cargar las secciones de Todocolección.")
        return

    path_state_key = _state_key(record_id, "path", state_key_prefix=state_key_prefix)
    selected_section_id = get_tc_section_value(
        record_id, state_key_prefix=state_key_prefix
    )
    selected_node = sections_index["leaf_by_section_id"].get(selected_section_id)
    selected_path = display_text((selected_node or {}).get("display_path"))

    with container.popover(
        f"Sección Todocolección · {selected_section_id}"
        if selected_section_id
        else "Sección Todocolección",
        use_container_width=True,
    ):
        stored_path = list(st.session_state.get(path_state_key) or [])
        parent_key = sections_index["root_key"]
        new_path = []
        selected_leaf_id = None

        for level in range(TC_SECTION_MAX_LEVELS):
            children = sections_index["children_by_parent"].get(parent_key, [])
            if not children:
                break

            option_keys = [""] + [child["node_key"] for child in children]
            default_key = ""
            if level < len(stored_path) and stored_path[level] in option_keys:
                default_key = stored_path[level]

            widget_key = _state_key(
                record_id, f"picker_{level}", state_key_prefix=state_key_prefix
            )
            if st.session_state.get(widget_key) not in option_keys:
                st.session_state[widget_key] = default_key

            choice = st.selectbox(
                f"Sección {level + 1}",
                option_keys,
                key=widget_key,
                on_change=_sync_tc_section_from_pickers,
                kwargs={
                    "record_id": record_id,
                    "sections_index": sections_index,
                    "state_key_prefix": state_key_prefix,
                },
                format_func=lambda value, current_level=level: (
                    "Sin sección"
                    if current_level == 0 and not value
                    else "Selecciona..."
                    if not value
                    else sections_index["nodes_by_key"][value]["label"]
                ),
                label_visibility="collapsed",
            )
            if not choice:
                break

            new_path.append(choice)
            node = sections_index["nodes_by_key"][choice]
            parent_key = choice
            if node["is_leaf"]:
                selected_leaf_id = node["section_id"]
                break

        st.session_state[path_state_key] = new_path
        st.session_state[
            _state_key(record_id, "value", state_key_prefix=state_key_prefix)
        ] = selected_leaf_id

    if selected_section_id:
        container.caption(selected_path or f"Sección {selected_section_id}")
    else:
        container.caption("Sin sección asignada.")
