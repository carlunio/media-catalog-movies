from pathlib import Path

from ..database import get_connection

COLUMNS = [
    "id",
    "manual_title",
    "extraction_title",
    "imdb_url",
    "imdb_id",
    "omdb_title",
    "omdb_year",
    "omdb_runtime",
    "omdb_genre",
    "omdb_director",
    "omdb_actors",
    "omdb_plot_en",
    "omdb_plot_es",
    "omdb_language",
    "omdb_country",
]


def export_movies_tsv(output_path: Path) -> Path:
    con = get_connection()

    rows = con.execute(
        f"SELECT {', '.join(COLUMNS)} FROM movies ORDER BY id"
    ).fetchall()
    con.close()

    lines: list[str] = ["\t".join(COLUMNS)]

    for row in rows:
        values = []
        for value in row:
            if value is None:
                clean = ""
            else:
                clean = str(value).replace("\t", " ").replace("\n", "\\n")
            values.append(clean)
        lines.append("\t".join(values))

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines), encoding="utf-8")
    return output_path
