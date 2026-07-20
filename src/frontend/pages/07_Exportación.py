from typing import Any

import streamlit as st

try:
    from src.frontend.utils import (
        LONG_TIMEOUT_SECONDS,
        api_get,
        api_get_bytes,
        api_post,
        configure_page,
        render_icon_heading,
        render_timeout_controls,
    )
except ModuleNotFoundError:  # pragma: no cover
    from frontend.utils import (
        LONG_TIMEOUT_SECONDS,
        api_get,
        api_get_bytes,
        api_post,
        configure_page,
        render_icon_heading,
        render_timeout_controls,
    )

configure_page()
render_icon_heading("Fase 7 - Exportación", icon="file-export", level=1)
render_timeout_controls()

SELECTION_COLUMN = "Seleccionar"
REFERENCE_COLUMN = "REFERENCIA"


def _sync_export_selection(
    preview_ids: list[str], default_selected_ids: list[str] | None = None
) -> list[str]:
    signature = (tuple(preview_ids), tuple(default_selected_ids or preview_ids))
    if st.session_state.get("movies_export_preview_signature") != signature:
        st.session_state["movies_export_preview_signature"] = signature
        st.session_state["movies_export_selected_ids"] = list(
            default_selected_ids if default_selected_ids is not None else preview_ids
        )
        st.session_state.pop("movies_export_selection_editor", None)

    selected_ids = [
        item_id
        for item_id in list(st.session_state.get("movies_export_selected_ids") or [])
        if item_id in preview_ids
    ]
    st.session_state["movies_export_selected_ids"] = selected_ids
    return selected_ids


def _selection_rows(
    preview_rows: list[dict[str, Any]], selected_ids: list[str]
) -> list[dict[str, Any]]:
    selected = set(selected_ids)
    return [
        {
            SELECTION_COLUMN: str(row.get(REFERENCE_COLUMN) or "").strip() in selected,
            **row,
        }
        for row in preview_rows
    ]


def _rows_from_editor_value(value: Any) -> list[dict[str, Any]]:
    if hasattr(value, "to_dict"):
        return list(value.to_dict(orient="records"))
    if isinstance(value, list):
        return [dict(row) for row in value]
    return []


def _validation_error_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "Referencia": row.get("id"),
            "Título": row.get("title"),
            "Errores": " ".join(str(item) for item in row.get("errors") or []),
        }
        for row in rows
        if not row.get("is_valid")
    ]


@st.dialog("Exportación completada", width="medium", dismissible=False)
def _export_result_dialog() -> None:
    export_bytes = st.session_state.get("movies_export_bytes")
    export_name = str(st.session_state.get("movies_export_filename") or "peliculas.csv")
    export_path = str(st.session_state.get("movies_export_path") or "")
    export_rows = int(st.session_state.get("movies_export_rows") or 0)
    export_ids = list(st.session_state.get("movies_export_ids") or [])

    st.success(f"Se ha generado `{export_name}` con {export_rows} filas.")
    if export_path:
        st.caption(f"Archivo guardado en `{export_path}`.")

    if isinstance(export_bytes, (bytes, bytearray)):
        st.download_button(
            label="Guardar también en este PC",
            data=bytes(export_bytes),
            file_name=export_name,
            mime="text/csv",
            type="primary",
            width="stretch",
        )

    secondary_col, primary_col = st.columns(2, gap="small")
    with secondary_col:
        if st.button("Cerrar sin cambiar estados", width="stretch"):
            st.session_state["movies_export_dialog_open"] = False
            st.rerun()
    with primary_col:
        if st.button("Quitar operación a exportados", width="stretch"):
            try:
                result = api_post(
                    "/export/movies/clear-operation",
                    json={"ids": export_ids},
                    timeout=LONG_TIMEOUT_SECONDS,
                )
            except Exception as exc:
                st.error(f"No se pudieron limpiar las operaciones: {exc}")
            else:
                updated = int(result.get("updated") or 0)
                st.session_state["movies_export_dialog_open"] = False
                st.session_state["movies_export_flash"] = (
                    f"Se ha quitado la operación a {updated} fichas exportadas."
                )
                st.rerun()


flash_message = str(st.session_state.pop("movies_export_flash", "") or "").strip()
if flash_message:
    st.success(flash_message)

try:
    catalog_items = api_get("/items", timeout=LONG_TIMEOUT_SECONDS)
except Exception:
    catalog_items = []

if not catalog_items:
    render_icon_heading("Preparación", icon="table-list", level=2)
    if st.button("Preparar fichas comerciales", type="primary"):
        try:
            result = api_post("/items/prepare", timeout=LONG_TIMEOUT_SECONDS)
        except Exception as exc:
            st.error(f"No se pudieron preparar las fichas: {exc}")
        else:
            created = int(result.get("created") or 0)
            st.session_state["movies_export_flash"] = (
                f"Se han preparado {created} fichas comerciales nuevas."
            )
            st.rerun()

try:
    preview = api_get("/export/movies/preview", timeout=LONG_TIMEOUT_SECONDS)
except Exception as exc:
    st.error(f"No se pudo cargar la vista previa: {exc}")
    st.stop()

preview_columns = list(preview.get("columns") or [])
preview_rows = list(preview.get("rows") or [])
preview_ids = list(preview.get("ids") or [])
preview_count = int(preview.get("rows_count") or len(preview_rows))
validation = dict(preview.get("validation") or {})
validation_rows = list(validation.get("rows") or [])
validation_by_id = {
    str(row.get("id") or "").strip(): row
    for row in validation_rows
    if str(row.get("id") or "").strip()
}
invalid_validation_rows = [
    row for row in validation_rows if not bool(row.get("is_valid"))
]
default_selected_ids = [str(item_id) for item_id in validation.get("valid_ids") or []]
selected_ids = _sync_export_selection(preview_ids, default_selected_ids)

summary_left, summary_right = st.columns([1, 2], gap="large")
with summary_left:
    st.metric(
        "Fichas exportables",
        int(validation.get("valid_count") or preview_count),
    )
with summary_right:
    st.caption("Plantilla Importamatic de Otros, UTF-8 y separador `#`.")
    if invalid_validation_rows:
        st.warning(
            f"{len(invalid_validation_rows)} fichas tienen avisos. "
            "Aparecen desmarcadas por defecto, pero puedes marcarlas y exportarlas igualmente."
        )
        with st.expander("Ver errores de validación"):
            st.dataframe(
                _validation_error_rows(invalid_validation_rows),
                hide_index=True,
                width="stretch",
            )

if preview_rows:
    controls_container = st.container()
    edited_rows = st.data_editor(
        _selection_rows(preview_rows, selected_ids),
        hide_index=True,
        use_container_width=True,
        disabled=preview_columns,
        column_config={
            SELECTION_COLUMN: st.column_config.CheckboxColumn(
                "Exportar",
                default=True,
                width="small",
            )
        },
        key="movies_export_selection_editor",
    )
    selected_ids = [
        str(row.get(REFERENCE_COLUMN) or "").strip()
        for row in _rows_from_editor_value(edited_rows)
        if bool(row.get(SELECTION_COLUMN))
        and str(row.get(REFERENCE_COLUMN) or "").strip()
    ]
    st.session_state["movies_export_selected_ids"] = selected_ids
    selected_invalid_rows = [
        validation_by_id[item_id]
        for item_id in selected_ids
        if item_id in validation_by_id and not validation_by_id[item_id].get("is_valid")
    ]
    all_selected = len(selected_ids) == len(preview_ids)
    export_disabled = not selected_ids

    with controls_container:
        toggle_col, status_col, export_col, covers_col, omdb_covers_col = st.columns(
            [1.2, 1.2, 0.8, 1, 1.1], gap="large"
        )
        with toggle_col:
            select_all = st.checkbox(
                "Seleccionar todas",
                value=all_selected,
                key=f"movies_export_select_all_{preview_count}_{int(all_selected)}",
            )
        with status_col:
            st.caption(f"Seleccionadas: {len(selected_ids)} de {preview_count}.")
            if selected_invalid_rows:
                st.warning(f"Seleccionadas con avisos: {len(selected_invalid_rows)}.")
        with export_col:
            export_requested = st.button(
                "Exportar CSV",
                type="primary",
                disabled=export_disabled,
                width="stretch",
            )
        with covers_col:
            covers_requested = st.button(
                "Preparar carátulas",
                disabled=export_disabled,
                width="stretch",
            )
        with omdb_covers_col:
            omdb_covers_requested = st.button(
                "Descargar imagen 2",
                disabled=export_disabled,
                width="stretch",
            )

        if select_all != all_selected:
            st.session_state["movies_export_selected_ids"] = (
                list(preview_ids) if select_all else []
            )
            st.session_state.pop("movies_export_selection_editor", None)
            st.rerun()
else:
    st.info("No hay fichas con operación `ALTA`, `CAMBIO` o `BAJA`.")
    selected_ids = []
    st.session_state["movies_export_selected_ids"] = []
    export_requested = False
    covers_requested = False
    omdb_covers_requested = False

if export_requested:
    with st.spinner("Generando archivo…"):
        try:
            result = api_post(
                "/export/movies/csv",
                json={"ids": selected_ids},
                timeout=LONG_TIMEOUT_SECONDS,
            )
            filename = str(result.get("filename") or "peliculas.csv")
            file_bytes = api_get_bytes(
                "/export/movies/file",
                params={"filename": filename},
                timeout=LONG_TIMEOUT_SECONDS,
            )
            st.session_state["movies_export_bytes"] = file_bytes
            st.session_state["movies_export_filename"] = filename
            st.session_state["movies_export_path"] = str(result.get("path") or "")
            st.session_state["movies_export_rows"] = int(result.get("rows") or 0)
            st.session_state["movies_export_ids"] = list(
                result.get("ids") or selected_ids
            )
            st.session_state["movies_export_dialog_open"] = True
            st.rerun()
        except Exception as exc:
            st.error(f"No se pudo exportar el catálogo: {exc}")

if covers_requested:
    with st.spinner("Preparando carátulas…"):
        try:
            result = api_post(
                "/export/movies/covers",
                json={"ids": selected_ids},
                timeout=LONG_TIMEOUT_SECONDS,
            )
        except Exception as exc:
            st.error(f"No se pudieron preparar las carátulas: {exc}")
        else:
            copied_count = int(result.get("copied_count") or 0)
            missing_count = int(result.get("missing_count") or 0)
            failed_count = int(result.get("failed_count") or 0)
            summary = (
                f"Carátulas preparadas: {copied_count}. "
                f"No encontradas: {missing_count}. Errores: {failed_count}."
            )
            if missing_count or failed_count:
                st.warning(summary)
            else:
                st.success(summary)
            covers_dir = str(result.get("covers_dir") or "")
            if covers_dir:
                st.caption(f"Carpeta: `{covers_dir}`.")
            failed = list(result.get("failed") or [])
            if failed:
                with st.expander("Ver errores"):
                    st.dataframe(failed, width="stretch", hide_index=True)

if omdb_covers_requested:
    with st.spinner("Descargando imagen 2 desde OMDb…"):
        try:
            result = api_post(
                "/omdb/covers/download",
                json={"ids": selected_ids},
                timeout=LONG_TIMEOUT_SECONDS,
            )
        except Exception as exc:
            st.error(f"No se pudieron descargar las imágenes 2: {exc}")
        else:
            downloaded_count = int(result.get("downloaded_count") or 0)
            skipped_count = int(result.get("skipped_count") or 0)
            failed_count = int(result.get("failed_count") or 0)
            summary = (
                f"Imágenes 2 descargadas: {downloaded_count}. "
                f"Saltadas: {skipped_count}. Errores: {failed_count}."
            )
            if failed_count or skipped_count:
                st.warning(summary)
            else:
                st.success(summary)
            output_dir = str(result.get("output_dir") or "")
            if output_dir:
                st.caption(f"Carpeta: `{output_dir}`.")
            failed = list(result.get("failed") or [])
            if failed:
                with st.expander("Ver errores de imagen 2"):
                    st.dataframe(failed, width="stretch", hide_index=True)

download_bytes = st.session_state.get("movies_export_bytes")
if isinstance(download_bytes, (bytes, bytearray)):
    st.divider()
    render_icon_heading("Última exportación", icon="download", level=2)
    st.download_button(
        label="Guardar una copia en este PC",
        data=bytes(download_bytes),
        file_name=str(
            st.session_state.get("movies_export_filename") or "peliculas.csv"
        ),
        mime="text/csv",
        width="stretch",
    )

if st.session_state.get("movies_export_dialog_open"):
    _export_result_dialog()
