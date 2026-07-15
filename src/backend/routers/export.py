from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse

from ..config import EXPORTS_DIR
from ..schemas.items import ExportItemsRequest
from ..schemas.omdb import DownloadOmdbCoversRequest
from ..services import export

router = APIRouter()


@router.get("/export/movies/preview")
def export_movies_preview():
    preview = export.get_export_preview()
    return {
        "ok": True,
        "columns": preview["columns"],
        "rows": preview["rows"],
        "ids": preview["ids"],
        "rows_count": int(preview["rows_count"]),
        "validation": preview["validation"],
    }


@router.get("/export/movies/validate")
def export_movies_validate_all():
    return export.validate_export_items()


@router.post("/export/movies/validate")
def export_movies_validate_selected(payload: ExportItemsRequest):
    return export.validate_export_items(ids=payload.ids)


def _export_csv_response(result: dict) -> dict:
    return {
        "ok": True,
        "path": str(result["path"]),
        "filename": str(result["filename"]),
        "rows": int(result["rows"]),
        "ids": result["ids"],
    }


@router.get("/export/movies/csv")
def export_movies_csv_all():
    try:
        return _export_csv_response(export.export_movies_csv())
    except export.ExportValidationError as exc:
        raise HTTPException(status_code=400, detail=exc.validation) from exc


@router.post("/export/movies/csv")
def export_movies_csv_selected(payload: ExportItemsRequest):
    try:
        return _export_csv_response(export.export_movies_csv(ids=payload.ids))
    except export.ExportValidationError as exc:
        raise HTTPException(status_code=400, detail=exc.validation) from exc


@router.get("/export/movies/file")
def export_movies_file(filename: str):
    name = str(filename or "").strip()
    if not name:
        raise HTTPException(
            status_code=400, detail="El nombre del archivo es obligatorio"
        )
    if "/" in name or "\\" in name:
        raise HTTPException(status_code=400, detail="Nombre de archivo no válido")
    if not name.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Solo se permiten archivos CSV")

    path = EXPORTS_DIR / name
    if not path.exists() or not path.is_file():
        raise HTTPException(
            status_code=404, detail="Archivo de exportación no encontrado"
        )
    return FileResponse(path=str(path), media_type="text/csv", filename=name)


@router.post("/export/movies/covers")
def export_movies_covers(payload: ExportItemsRequest):
    return {"ok": True, **export.export_cover_images(ids=payload.ids)}


@router.post("/export/movies/clear-operation")
def export_movies_clear_operation(payload: ExportItemsRequest):
    result = export.clear_exported_items_listing_status(payload.ids)
    return {
        "ok": True,
        "updated": int(result["updated"]),
        "ids": result["ids"],
    }


@router.post("/omdb/covers/download")
def download_omdb_covers(payload: DownloadOmdbCoversRequest):
    ids = list(payload.ids) if payload.ids is not None else None
    if payload.movie_id:
        ids = [payload.movie_id, *(ids or [])]
    return export.download_omdb_second_images(ids=ids, poster_slot=payload.poster_slot)


@router.get("/export/covers/originals")
def export_original_covers():
    return export.export_original_covers()
