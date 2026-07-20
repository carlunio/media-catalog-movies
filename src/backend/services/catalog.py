from contextlib import closing
from typing import Any

from ..config import IMPORTAMATIC_OTHERS_FIXED_COST_EXPORT, TC_SECTIONS_CSV_PATH
from ..database import get_connection
from ..repositories import items_repo
from .tc_sections import build_tc_section_nodes, normalize_tc_section_value

ITEMS_TABLE = items_repo.TABLE_NAME
EXPORT_VIEW_NAME = "export"
EXPORT_REFERENCE_COLUMN = "REFERENCIA"
ALLOWED_VALUES_TABLE = "inventory_field_allowed_values"
TC_SECTIONS_TABLE = "tc_sections"
EXPORTABLE_LISTING_STATUSES = ("ALTA", "CAMBIO", "BAJA")
IMPORTAMATIC_EXPORT_COLUMNS = [
    "REFERENCIA",
    "TÍTULO",
    "DESCRIPCIÓN",
    "AUTOR ",
    "PRECIO",
    "OPERACIÓN",
    "SECCIÓN",
    "ESTADO",
    "DESCRIPCIÓN DEL ESTADO",
    "IMAGEN 1 (principal)",
    "IMAGEN 2",
    "IMAGEN 3",
    "FORMA DE ENVÍO",
    "GASTOS FIJOS",
]

ITEM_COLUMNS = items_repo.COLUMNS
EDITABLE_COLUMNS = items_repo.EDITABLE_COLUMNS
OPTION_FIELDS = {"listing_status", "stock_status", "tc_condition"}
ALLOWED_VALUES = (
    ("tc_condition", "5"),
    ("tc_condition", "4"),
    ("tc_condition", "3"),
    ("tc_condition", "2"),
    ("tc_condition", "1"),
    ("listing_status", "ALTA"),
    ("listing_status", "CAMBIO"),
    ("listing_status", "BAJA"),
    ("stock_status", "En stock"),
    ("stock_status", "Vendido"),
    ("stock_status", "Extraviado"),
)


def _clean_optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _quote_identifier(identifier: str) -> str:
    return '"' + str(identifier).replace('"', '""') + '"'


def _clean_sql_text(expression: str) -> str:
    return (
        "NULLIF(TRIM(REGEXP_REPLACE("
        f"CAST({expression} AS VARCHAR), '[\\r\\n\\t]+', ' / ', 'g'"
        ")), '')"
    )


def _html_clean_sql_text(expression: str, *, preserve_line_breaks: bool = False) -> str:
    normalized_tabs = (
        f"REGEXP_REPLACE(CAST({expression} AS VARCHAR), '[\\t]+', ' ', 'g')"
    )
    if preserve_line_breaks:
        normalized_text = (
            f"REPLACE(REPLACE({normalized_tabs}, CHR(13) || CHR(10), CHR(10)), "
            "CHR(13), CHR(10))"
        )
    else:
        normalized_text = f"REGEXP_REPLACE({normalized_tabs}, '[\\r\\n]+', ' / ', 'g')"
    return f"NULLIF(TRIM({normalized_text}), '')"


def _html_escape_sql(expression: str, *, preserve_line_breaks: bool = False) -> str:
    clean_expression = _html_clean_sql_text(
        expression, preserve_line_breaks=preserve_line_breaks
    )
    return (
        "REPLACE(REPLACE(REPLACE("
        f"{clean_expression}, "
        "'&', '&amp;'), '<', '&lt;'), '>', '&gt;')"
    )


def _html_paragraph_sql(
    label: str, expression: str, *, preserve_line_breaks: bool = False
) -> str:
    escaped_label = label.replace("'", "''")
    clean_expression = _html_escape_sql(
        expression, preserve_line_breaks=preserve_line_breaks
    )
    content_expression = (
        f"REPLACE(REPLACE({clean_expression}, CHR(10) || CHR(10), "
        "'<br><br>'), CHR(10), '<br>')"
        if preserve_line_breaks
        else clean_expression
    )
    return (
        f"CASE WHEN {clean_expression} IS NOT NULL "
        f"THEN '<p><strong>{escaped_label}:</strong> ' || "
        f"{content_expression} || '</p>' END"
    )


def _html_block_paragraph_sql(label: str, expression: str) -> str:
    escaped_label = label.replace("'", "''")
    clean_expression = _html_escape_sql(expression, preserve_line_breaks=True)
    content_expression = (
        f"REPLACE(REPLACE({clean_expression}, CHR(10) || CHR(10), "
        "'<br><br>'), CHR(10), '<br>')"
    )
    return (
        f"CASE WHEN {clean_expression} IS NOT NULL "
        f"THEN '<p><strong>{escaped_label}:</strong></p><p>' || "
        f"{content_expression} || '</p>' END"
    )


def _tc_export_title_sql() -> str:
    title = _clean_sql_text("item.title")
    details = (
        f"CONCAT_WS(', ', {_clean_sql_text('item.director')}, "
        f"{_clean_sql_text('item.year')})"
    )
    return (
        f"CASE WHEN {title} IS NULL THEN NULL "
        f"WHEN NULLIF({details}, '') IS NULL THEN {title} "
        f"ELSE {title} || ' (' || {details} || ')' END"
    )


def _tc_description_sql() -> str:
    description = (
        "CONCAT_WS('', "
        f"{_html_paragraph_sql('Título original', 'item.original_title')}, "
        f"{_html_paragraph_sql('Año', 'item.year')}, "
        f"{_html_paragraph_sql('Tipo', 'item.item_type')}, "
        f"{_html_paragraph_sql('Director', 'item.director')}, "
        f"{_html_paragraph_sql('Guion', 'item.writers')}, "
        f"{_html_paragraph_sql('Reparto', 'item.actors')}, "
        f"{_html_paragraph_sql('Duración', 'item.runtime')}, "
        f"{_html_paragraph_sql('Géneros', 'item.genres')}, "
        f"{_html_paragraph_sql('País', 'item.country')}, "
        f"{_html_paragraph_sql('Idiomas', 'item.languages')}, "
        f"{_html_block_paragraph_sql('Sinopsis', 'item.plot')}, "
        f"{_html_block_paragraph_sql('Notas', 'item.notes')}"
        ")"
    )
    fallback = f"'<p>' || {_html_escape_sql('item.title')} || '</p>'"
    return f"COALESCE(NULLIF({description}, ''), {fallback}, '')"


def _tc_condition_description_sql() -> str:
    description = _clean_sql_text("item.condition_comments")
    return (
        f"CASE WHEN {description} IS NULL THEN NULL "
        f"WHEN REGEXP_MATCHES({description}, '[.!?]$') THEN {description} "
        f"ELSE {description} || '.' END"
    )


def _export_image_filename_sql() -> str:
    extension = (
        "LOWER(REGEXP_EXTRACT(COALESCE(item.image_path, ''), " "'[.][A-Za-z0-9]+$', 0))"
    )
    return (
        f"CASE WHEN {extension} IN ('.jpg', '.jpeg', '.png', '.webp', '.gif') "
        f"THEN item.id || {extension} ELSE item.id || '.jpg' END"
    )



def _ensure_allowed_values_table(con) -> None:
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {ALLOWED_VALUES_TABLE} (
            table_name TEXT,
            field_name TEXT,
            field_value TEXT,
            sort_order INTEGER DEFAULT 0,
            PRIMARY KEY (table_name, field_name, field_value)
        )
        """)
    column_additions = (
        ("table_name", "TEXT"),
        ("field_name", "TEXT"),
        ("field_value", "TEXT"),
        ("sort_order", "INTEGER DEFAULT 0"),
    )
    for column_name, column_type in column_additions:
        con.execute(
            f"ALTER TABLE {ALLOWED_VALUES_TABLE} "
            f"ADD COLUMN IF NOT EXISTS {column_name} {column_type}"
        )


def _sync_allowed_values_table(con) -> None:
    _ensure_allowed_values_table(con)
    con.execute(
        f"DELETE FROM {ALLOWED_VALUES_TABLE} WHERE table_name = ?",
        (ITEMS_TABLE,),
    )
    con.executemany(
        f"""
        INSERT INTO {ALLOWED_VALUES_TABLE} (
            table_name, field_name, field_value, sort_order
        )
        VALUES (?, ?, ?, ?)
        """,
        [
            (ITEMS_TABLE, field_name, field_value, index)
            for index, (field_name, field_value) in enumerate(ALLOWED_VALUES)
        ],
    )


def _ensure_tc_sections_table(con) -> None:
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {TC_SECTIONS_TABLE} (
            node_key TEXT PRIMARY KEY,
            parent_key TEXT,
            section_id TEXT,
            label TEXT NOT NULL,
            depth INTEGER NOT NULL,
            path_labels VARCHAR[],
            path_keys VARCHAR[],
            path_text TEXT NOT NULL,
            display_path TEXT NOT NULL,
            is_leaf BOOLEAN NOT NULL,
            sort_order INTEGER DEFAULT 0
        )
        """)
    column_additions = (
        ("parent_key", "TEXT"),
        ("section_id", "TEXT"),
        ("label", "TEXT"),
        ("depth", "INTEGER"),
        ("path_labels", "VARCHAR[]"),
        ("path_keys", "VARCHAR[]"),
        ("path_text", "TEXT"),
        ("display_path", "TEXT"),
        ("is_leaf", "BOOLEAN"),
        ("sort_order", "INTEGER DEFAULT 0"),
    )
    for column_name, column_type in column_additions:
        con.execute(
            f"ALTER TABLE {TC_SECTIONS_TABLE} "
            f"ADD COLUMN IF NOT EXISTS {column_name} {column_type}"
        )
    con.execute(f"ALTER TABLE {TC_SECTIONS_TABLE} ALTER COLUMN section_id TYPE TEXT")


def _sync_tc_sections_table(con) -> None:
    _ensure_tc_sections_table(con)
    nodes = build_tc_section_nodes(TC_SECTIONS_CSV_PATH)
    con.execute(f"DELETE FROM {TC_SECTIONS_TABLE}")
    if not nodes:
        return
    con.executemany(
        f"""
        INSERT INTO {TC_SECTIONS_TABLE} (
            node_key, parent_key, section_id, label, depth, path_labels,
            path_keys, path_text, display_path, is_leaf, sort_order
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                node["node_key"],
                node["parent_key"],
                node["section_id"],
                node["label"],
                node["depth"],
                node["path_labels"],
                node["path_keys"],
                node["path_text"],
                node["display_path"],
                node["is_leaf"],
                node["sort_order"],
            )
            for node in nodes
        ],
    )


def _ensure_export_view(con) -> None:
    price_sql = "REPLACE(CAST(item.sale_price AS VARCHAR), '.', ',')"
    select_expression_by_column = {
        EXPORT_REFERENCE_COLUMN: "item.id",
        "TÍTULO": f"LEFT({_tc_export_title_sql()}, 100)",
        "DESCRIPCIÓN": _tc_description_sql(),
        "AUTOR ": "LEFT(item.director, 100)",
        "PRECIO": price_sql,
        "OPERACIÓN": "item.listing_status",
        "SECCIÓN": "item.tc_section",
        "ESTADO": "item.tc_condition",
        "DESCRIPCIÓN DEL ESTADO": (f"LEFT({_tc_condition_description_sql()}, 100)"),
        "IMAGEN 1 (principal)": _export_image_filename_sql(),
        "IMAGEN 2": "item.id || '_2.jpg'",
        "IMAGEN 3": "NULL",
        "FORMA DE ENVÍO": "'Otros'",
        "GASTOS FIJOS": f"'{IMPORTAMATIC_OTHERS_FIXED_COST_EXPORT}'",
    }
    select_sql = ",\n".join(
        "            "
        f"{select_expression_by_column[column]} AS {_quote_identifier(column)}"
        for column in IMPORTAMATIC_EXPORT_COLUMNS
    )
    status_values = ", ".join(f"'{value}'" for value in EXPORTABLE_LISTING_STATUSES)
    con.execute(f"""
        CREATE OR REPLACE VIEW {_quote_identifier(EXPORT_VIEW_NAME)} AS
        SELECT
{select_sql}
        FROM {ITEMS_TABLE} AS item
        WHERE item.listing_status IN ({status_values})
        """)


def ensure_schema(con) -> None:
    items_repo.ensure_table(con)
    items_repo.backfill_omdb_structured_fields(con)
    items_repo.normalize_translated_fields(con)
    items_repo.normalize_image_paths(con)
    _sync_allowed_values_table(con)
    _sync_tc_sections_table(con)
    _ensure_export_view(con)


def init_table() -> None:
    with closing(get_connection()) as con:
        ensure_schema(con)


def prepare() -> int:
    with closing(get_connection()) as con:
        created = items_repo.insert_missing_from_movies(con)
        items_repo.refresh_generated_titles_from_movies(con)
        items_repo.backfill_omdb_structured_fields(con)
        items_repo.normalize_translated_fields(con)
        items_repo.normalize_image_paths(con)
        return created


def list_items() -> list[dict[str, Any]]:
    with closing(get_connection()) as con:
        return items_repo.list_records(con)


def get_item(item_id: str) -> dict[str, Any] | None:
    with closing(get_connection()) as con:
        return items_repo.get_record(con, item_id)


def update_item(item_id: str, fields: dict[str, Any]) -> dict[str, Any]:
    updates = {key: value for key, value in fields.items() if key in EDITABLE_COLUMNS}
    if "tc_section" in updates:
        updates["tc_section"] = normalize_tc_section_value(updates["tc_section"])
    if "image_path" in updates:
        updates["image_path"] = items_repo.normalize_image_path_value(updates["image_path"])
    for field_name in EDITABLE_COLUMNS - {"sale_price", "tc_section"}:
        if field_name in updates:
            updates[field_name] = _clean_optional_text(updates[field_name])

    with closing(get_connection()) as con:
        if not items_repo.exists(con, item_id):
            raise ValueError(f"La ficha {item_id} no existe")
        items_repo.update_fields(con, item_id, updates)
        item = items_repo.get_record(con, item_id)
    if item is None:
        raise ValueError(f"La ficha {item_id} no existe")
    return item


def get_allowed_values() -> dict[str, list[str]]:
    with closing(get_connection()) as con:
        rows = con.execute(
            f"""
            SELECT field_name, field_value
            FROM {ALLOWED_VALUES_TABLE}
            WHERE table_name = ?
            ORDER BY field_name, sort_order, field_value
            """,
            (ITEMS_TABLE,),
        ).fetchall()
    grouped: dict[str, list[str]] = {}
    for field_name, field_value in rows:
        name = str(field_name or "").strip()
        value = str(field_value or "").strip()
        if name in OPTION_FIELDS and value:
            grouped.setdefault(name, []).append(value)
    return grouped


def get_tc_sections_catalog() -> dict[str, Any]:
    with closing(get_connection()) as con:
        rows = con.execute(f"""
            SELECT node_key, parent_key, section_id, label, depth, path_labels,
                   path_keys, path_text, display_path, is_leaf, sort_order
            FROM {TC_SECTIONS_TABLE}
            ORDER BY depth, sort_order, path_text
            """).fetchall()
    nodes = [
        {
            "node_key": row[0],
            "parent_key": row[1],
            "section_id": row[2],
            "label": row[3],
            "depth": int(row[4] or 0),
            "path_labels": list(row[5] or []),
            "path_keys": list(row[6] or []),
            "path_text": row[7],
            "display_path": row[8],
            "is_leaf": bool(row[9]),
            "sort_order": int(row[10] or 0),
        }
        for row in rows
    ]
    root_key = next(
        (node["node_key"] for node in nodes if int(node["depth"]) == 0),
        None,
    )
    return {"root_key": root_key, "nodes": nodes}
