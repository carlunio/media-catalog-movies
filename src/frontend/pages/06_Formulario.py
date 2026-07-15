from pathlib import Path
from typing import Any

import streamlit as st
from PIL import Image, ImageOps

try:
    from src.frontend.tc_sections import (
        build_tc_sections_index,
        get_tc_section_value,
        render_tc_section_selector,
        sync_tc_section_state,
    )
    from src.frontend.utils import (
        LONG_TIMEOUT_SECONDS,
        api_get,
        api_post,
        api_put,
        configure_page,
        get_selected_movie_id,
        render_icon_heading,
        render_timeout_controls,
        set_selected_movie_id,
    )
except ModuleNotFoundError:  # pragma: no cover
    from frontend.tc_sections import (
        build_tc_sections_index,
        get_tc_section_value,
        render_tc_section_selector,
        sync_tc_section_state,
    )
    from frontend.utils import (
        LONG_TIMEOUT_SECONDS,
        api_get,
        api_post,
        api_put,
        configure_page,
        get_selected_movie_id,
        render_icon_heading,
        render_timeout_controls,
        set_selected_movie_id,
    )


configure_page()
render_icon_heading("Fase 6 - Formulario", icon="clipboard-list", level=1)
render_timeout_controls()

EXPORTABLE_LISTING_STATUSES = {"ALTA", "CAMBIO", "BAJA"}
ITEM_SELECTOR_KEY = "movies_catalog_selected_item_id"
PENDING_ITEM_SELECTOR_KEY = "movies_catalog_pending_item_id"


def _display_text(value: Any) -> str:
    text = str(value or "").strip()
    if text.lower() in {"none", "null", "nan"}:
        return ""
    return text


def _field_key(item_id: str, field: str) -> str:
    return f"movies_catalog_{item_id}_{field}"


def _safe_index(options: list[str], value: str | None, default: int = 0) -> int:
    try:
        return options.index(str(value or ""))
    except ValueError:
        return default


def _field_options(
    allowed_values: dict[str, list[str]], field: str, current: Any
) -> list[str]:
    options = [""]
    for item in allowed_values.get(field, []):
        value = _display_text(item)
        if value and value not in options:
            options.append(value)

    current_value = _display_text(current)
    if current_value and current_value not in options:
        options.append(current_value)
    return options


def _tc_condition_options(
    allowed_values: dict[str, list[str]], current: Any
) -> list[str]:
    options: list[str] = []
    for item in allowed_values.get("tc_condition", []) or ["5", "4", "3", "2", "1"]:
        value = _display_text(item)
        if value and value not in options:
            options.append(value)

    current_value = _display_text(current)
    if current_value and current_value not in options:
        options.append(current_value)
    if "" not in options:
        options.append("")
    return options


def _parse_optional_price(raw: str) -> float | None:
    text = _display_text(raw).replace(",", ".")
    if not text:
        return None
    return float(text)


def _price_input_value(value: Any) -> float:
    text = _display_text(value).replace(",", ".")
    if not text:
        return 0.0
    try:
        return max(0.0, float(text))
    except (TypeError, ValueError):
        return 0.0


def _resolve_image_path(raw_path: Any) -> Path | None:
    text = _display_text(raw_path)
    if not text or text.startswith(("http://", "https://")):
        return None

    path = Path(text).expanduser()
    if not path.is_absolute():
        path = Path.cwd() / path
    return path.resolve()


def _load_image_with_orientation(path: Path):
    with Image.open(path) as image:
        return ImageOps.exif_transpose(image).copy()


def _render_cover(item: dict[str, Any]) -> None:
    image_text = _display_text(item.get("image_path"))
    if image_text.startswith(("http://", "https://")):
        st.image(image_text, width="stretch")
        return

    image_path = _resolve_image_path(image_text)
    if image_path and image_path.exists():
        try:
            st.image(_load_image_with_orientation(image_path), width="stretch")
        except (OSError, ValueError) as exc:
            st.warning(f"No se pudo cargar la carátula: {exc}")
            st.caption(f"Ruta guardada: `{image_text}`.")
    else:
        st.info("Sin carátula disponible.")
        if image_text:
            st.caption(f"Ruta guardada: `{image_text}`.")


def _item_label(row: dict[str, Any]) -> str:
    item_id = _display_text(row.get("id")) or "?"
    title = _display_text(row.get("title")) or "(sin título)"
    year = _display_text(row.get("year"))
    status = _display_text(row.get("listing_status")) or "sin operación"
    suffix = f" · {year}" if year else ""
    return f"{item_id} | {title}{suffix} | {status}"


def _has_price(row: dict[str, Any]) -> bool:
    try:
        return row.get("sale_price") is not None and float(row.get("sale_price")) > 0
    except (TypeError, ValueError):
        return False


def _is_ready_to_export(row: dict[str, Any]) -> bool:
    return (
        _display_text(row.get("title")) != ""
        and _has_price(row)
        and _display_text(row.get("listing_status")) in EXPORTABLE_LISTING_STATUSES
        and _display_text(row.get("tc_section")) != ""
        and _display_text(row.get("tc_condition")) != ""
    )


def _filter_items(rows: list[dict[str, Any]], filter_label: str) -> list[dict[str, Any]]:
    if filter_label == "Sin precio":
        return [row for row in rows if not _has_price(row)]
    if filter_label == "Sin sección":
        return [row for row in rows if not _display_text(row.get("tc_section"))]
    if filter_label == "Sin estado TC":
        return [row for row in rows if not _display_text(row.get("tc_condition"))]
    if filter_label == "Sin operación":
        return [row for row in rows if not _display_text(row.get("listing_status"))]
    if filter_label == "Con operación de exportación":
        return [
            row
            for row in rows
            if _display_text(row.get("listing_status")) in EXPORTABLE_LISTING_STATUSES
        ]
    if filter_label == "Listas para exportar":
        return [row for row in rows if _is_ready_to_export(row)]
    return rows


def _select_item_id(rows: list[dict[str, Any]]) -> str:
    item_ids = [
        _display_text(row.get("id")) for row in rows if _display_text(row.get("id"))
    ]
    pending = _display_text(st.session_state.pop(PENDING_ITEM_SELECTOR_KEY, ""))
    preferred = get_selected_movie_id()
    current = _display_text(st.session_state.get(ITEM_SELECTOR_KEY))
    if pending in item_ids:
        st.session_state[ITEM_SELECTOR_KEY] = pending
    elif current not in item_ids:
        st.session_state[ITEM_SELECTOR_KEY] = (
            preferred if preferred in item_ids else item_ids[0]
        )

    labels = {_display_text(row.get("id")): _item_label(row) for row in rows}
    selected_id = st.selectbox(
        "Selecciona una ficha",
        item_ids,
        key=ITEM_SELECTOR_KEY,
        format_func=lambda value: labels.get(value, value),
    )
    set_selected_movie_id(selected_id)
    return selected_id


try:
    options_payload = api_get("/items/options", timeout=LONG_TIMEOUT_SECONDS)
except Exception as exc:
    st.error(f"No se pudieron cargar las opciones comerciales: {exc}")
    st.stop()

allowed_values = (
    options_payload.get("allowed_values") if isinstance(options_payload, dict) else {}
)
if not isinstance(allowed_values, dict):
    allowed_values = {}
sections_index = build_tc_sections_index(
    options_payload.get("tc_sections") if isinstance(options_payload, dict) else {}
)

try:
    rows = api_get("/items", timeout=LONG_TIMEOUT_SECONDS)
except Exception as exc:
    st.error(f"No se pudo cargar el catálogo comercial: {exc}")
    st.stop()

if not rows:
    st.info("Todavía no hay fichas comerciales preparadas.")
    if st.button("Preparar fichas comerciales", type="primary"):
        try:
            result = api_post("/items/prepare", timeout=LONG_TIMEOUT_SECONDS)
        except Exception as exc:
            st.error(f"No se pudieron preparar las fichas: {exc}")
        else:
            created = int(result.get("created") or 0)
            st.success(f"Se han preparado {created} fichas comerciales nuevas.")
            st.rerun()
    st.stop()

rows = [dict(row) for row in rows]
total = len(rows)
without_price = sum(1 for row in rows if not _has_price(row))
without_section = sum(1 for row in rows if not _display_text(row.get("tc_section")))
ready_count = sum(1 for row in rows if _is_ready_to_export(row))

metric_total, metric_price, metric_section, metric_ready = st.columns(4, gap="small")
metric_total.metric("Fichas", total)
metric_price.metric("Sin precio", without_price)
metric_section.metric("Sin sección", without_section)
metric_ready.metric("Listas", ready_count)

filter_label = st.segmented_control(
    "Filtro",
    [
        "Todas",
        "Sin precio",
        "Sin sección",
        "Sin estado TC",
        "Sin operación",
        "Con operación de exportación",
        "Listas para exportar",
    ],
    default="Todas",
)
filtered_rows = _filter_items(rows, str(filter_label or "Todas"))

if not filtered_rows:
    st.info("No hay fichas para el filtro seleccionado.")
    st.stop()

selected_id = _select_item_id(filtered_rows)
current_index = [_display_text(row.get("id")) for row in filtered_rows].index(
    selected_id
)

nav_left, nav_center, nav_right = st.columns([1, 2, 1], gap="small")
with nav_left:
    if st.button("Anterior", disabled=current_index == 0, width="stretch"):
        previous_id = _display_text(filtered_rows[current_index - 1].get("id"))
        st.session_state[PENDING_ITEM_SELECTOR_KEY] = previous_id
        set_selected_movie_id(previous_id)
        st.rerun()
with nav_center:
    st.caption(f"Ficha {current_index + 1} de {len(filtered_rows)} en este filtro.")
with nav_right:
    if st.button(
        "Siguiente",
        disabled=current_index >= len(filtered_rows) - 1,
        width="stretch",
    ):
        next_id = _display_text(filtered_rows[current_index + 1].get("id"))
        st.session_state[PENDING_ITEM_SELECTOR_KEY] = next_id
        set_selected_movie_id(next_id)
        st.rerun()

try:
    item = api_get(f"/items/{selected_id}", timeout=LONG_TIMEOUT_SECONDS)
except Exception as exc:
    st.error(f"No se pudo cargar la ficha {selected_id}: {exc}")
    st.stop()

sync_tc_section_state(
    selected_id,
    item.get("tc_section"),
    sections_index,
    state_key_prefix="movies_catalog",
)

listing_status_options = _field_options(
    allowed_values, "listing_status", item.get("listing_status")
)
stock_status_options = _field_options(
    allowed_values, "stock_status", item.get("stock_status")
)
tc_condition_options = _tc_condition_options(allowed_values, item.get("tc_condition"))

st.divider()
left_col, main_col, extra_col = st.columns([0.9, 1.35, 1.15], gap="large")

with left_col:
    render_icon_heading("Portada", icon="image", level=2)
    _render_cover(item)
    st.text_input(
        "Referencia",
        value=selected_id,
        disabled=True,
        key=_field_key(selected_id, "id"),
    )
    image_path = st.text_input(
        "Ruta de carátula",
        value=item.get("image_path") or "",
        key=_field_key(selected_id, "image_path"),
    )

with main_col:
    render_icon_heading("Ficha", icon="film", level=2)
    title = st.text_input(
        "Título",
        value=item.get("title") or "",
        key=_field_key(selected_id, "title"),
    )
    original_title = st.text_input(
        "Título original",
        value=item.get("original_title") or "",
        key=_field_key(selected_id, "original_title"),
    )
    director = st.text_input(
        "Director",
        value=item.get("director") or "",
        key=_field_key(selected_id, "director"),
    )
    item_type = st.text_input(
        "Tipo",
        value=item.get("item_type") or "",
        key=_field_key(selected_id, "item_type"),
    )
    year_col, runtime_col = st.columns(2, gap="small")
    year = year_col.text_input(
        "Año",
        value=item.get("year") or "",
        key=_field_key(selected_id, "year"),
    )
    runtime = runtime_col.text_input(
        "Duración",
        value=item.get("runtime") or "",
        key=_field_key(selected_id, "runtime"),
    )
    genres = st.text_input(
        "Géneros",
        value=item.get("genres") or "",
        key=_field_key(selected_id, "genres"),
    )
    actors = st.text_area(
        "Reparto",
        value=item.get("actors") or "",
        height=90,
        key=_field_key(selected_id, "actors"),
    )
    plot = st.text_area(
        "Sinopsis",
        value=item.get("plot") or "",
        height=210,
        key=_field_key(selected_id, "plot"),
    )

with extra_col:
    render_icon_heading("Venta", icon="tags", level=2)
    sale_price_raw = st.number_input(
        "Precio (€)",
        min_value=0.0,
        step=0.5,
        value=_price_input_value(item.get("sale_price")),
        format="%.2f",
        key=_field_key(selected_id, "sale_price"),
    )
    listing_status = st.selectbox(
        "Operación",
        listing_status_options,
        index=_safe_index(
            listing_status_options,
            _display_text(item.get("listing_status")),
            default=0,
        ),
        key=_field_key(selected_id, "listing_status"),
    )
    stock_status = st.selectbox(
        "Estado de stock",
        stock_status_options,
        index=_safe_index(
            stock_status_options,
            _display_text(item.get("stock_status")),
            default=0,
        ),
        key=_field_key(selected_id, "stock_status"),
    )
    render_tc_section_selector(selected_id, sections_index, state_key_prefix="movies_catalog")
    tc_condition = st.selectbox(
        "Estado TC",
        tc_condition_options,
        index=_safe_index(
            tc_condition_options,
            _display_text(item.get("tc_condition")),
            default=len(tc_condition_options) - 1,
        ),
        key=_field_key(selected_id, "tc_condition"),
        help="5 Muy bueno; 4 Bueno; 3 Normal; 2 Algún defecto; 1 Defectuoso.",
    )
    condition_comments = st.text_area(
        "Descripción del estado",
        value=item.get("condition_comments") or "",
        height=100,
        key=_field_key(selected_id, "condition_comments"),
    )
    notes = st.text_area(
        "Notas para la descripción",
        value=item.get("notes") or "",
        height=150,
        key=_field_key(selected_id, "notes"),
    )

details_left, details_right = st.columns(2, gap="large")
with details_left:
    writers = st.text_area(
        "Guion",
        value=item.get("writers") or "",
        height=80,
        key=_field_key(selected_id, "writers"),
    )
    country = st.text_input(
        "País",
        value=item.get("country") or "",
        key=_field_key(selected_id, "country"),
    )
    languages = st.text_input(
        "Idiomas",
        value=item.get("languages") or "",
        key=_field_key(selected_id, "languages"),
    )
    released = st.text_input(
        "Estreno",
        value=item.get("released") or "",
        key=_field_key(selected_id, "released"),
    )
with details_right:
    production = st.text_input(
        "Productora",
        value=item.get("production") or "",
        key=_field_key(selected_id, "production"),
    )
    awards = st.text_area(
        "Premios",
        value=item.get("awards") or "",
        height=80,
        key=_field_key(selected_id, "awards"),
    )
    imdb_url = st.text_input(
        "Enlace IMDb",
        value=item.get("imdb_url") or "",
        key=_field_key(selected_id, "imdb_url"),
    )
    rating_col, votes_col, box_col = st.columns(3, gap="small")
    imdb_rating = rating_col.text_input(
        "IMDb",
        value=item.get("imdb_rating") or "",
        key=_field_key(selected_id, "imdb_rating"),
    )
    imdb_votes = votes_col.text_input(
        "Votos",
        value=item.get("imdb_votes") or "",
        key=_field_key(selected_id, "imdb_votes"),
    )
    box_office = box_col.text_input(
        "Taquilla",
        value=item.get("box_office") or "",
        key=_field_key(selected_id, "box_office"),
    )

save = st.button("Guardar cambios", type="primary", width="stretch")

if save:
    try:
        sale_price = _parse_optional_price(sale_price_raw)
    except ValueError:
        st.error("El precio debe ser un número válido.")
        st.stop()

    payload = {
        "title": title,
        "original_title": original_title,
        "item_type": item_type,
        "director": director,
        "writers": writers,
        "actors": actors,
        "year": year,
        "released": released,
        "runtime": runtime,
        "genres": genres,
        "country": country,
        "languages": languages,
        "plot": plot,
        "awards": awards,
        "production": production,
        "imdb_url": imdb_url,
        "imdb_rating": imdb_rating,
        "imdb_votes": imdb_votes,
        "box_office": box_office,
        "sale_price": sale_price,
        "listing_status": listing_status,
        "stock_status": stock_status,
        "tc_section": get_tc_section_value(
            selected_id, state_key_prefix="movies_catalog"
        ),
        "tc_condition": tc_condition,
        "condition_comments": condition_comments,
        "notes": notes,
        "image_path": image_path,
    }

    try:
        api_put(f"/items/{selected_id}", json=payload, timeout=LONG_TIMEOUT_SECONDS)
    except Exception as exc:
        st.error(f"No se pudo actualizar la ficha: {exc}")
    else:
        st.success("Ficha actualizada correctamente.")
        st.rerun()
