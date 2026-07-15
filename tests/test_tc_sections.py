from pathlib import Path

from src.backend.services.tc_sections import (
    build_tc_section_nodes,
    normalize_tc_section_value,
)


def test_build_tc_section_nodes_uses_cinema_root_and_leaf_ids(tmp_path: Path):
    csv_path = tmp_path / "secciones.csv"
    csv_path.write_text(
        "id sección,título\n"
        "434,Cine - Películas - DVD\n"
        "447,Series TV en DVD\n"
        "bad,\n",
        encoding="utf-8",
    )

    nodes = build_tc_section_nodes(csv_path)
    leaves = {
        str(node["section_id"]): node
        for node in nodes
        if node.get("section_id") is not None
    }

    assert leaves["434"]["path_labels"] == [
        "Cine y televisión",
        "Películas",
        "DVD",
    ]
    assert leaves["434"]["display_path"] == "Películas > DVD"
    assert leaves["447"]["path_labels"] == [
        "Cine y televisión",
        "Series TV en DVD",
    ]


def test_normalize_tc_section_value_accepts_only_stable_ids():
    assert normalize_tc_section_value(434) == "434"
    assert normalize_tc_section_value(434.0) == "434"
    assert normalize_tc_section_value(" 447 ") == "447"
    assert normalize_tc_section_value(None) is None
    assert normalize_tc_section_value(True) is None
    assert normalize_tc_section_value(4.34) is None
    assert normalize_tc_section_value("nan") is None
