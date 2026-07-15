from fastapi import APIRouter, HTTPException

from ..schemas.items import ItemUpdateRequest
from ..services import catalog

router = APIRouter()


@router.post("/items/prepare")
def prepare_items():
    return {"created": catalog.prepare()}


@router.get("/items")
def list_items():
    return catalog.list_items()


@router.get("/items/options")
def item_options():
    return {
        "allowed_values": catalog.get_allowed_values(),
        "tc_sections": catalog.get_tc_sections_catalog(),
    }


@router.get("/items/{item_id}")
def get_item(item_id: str):
    item = catalog.get_item(item_id)
    if item is None:
        raise HTTPException(status_code=404, detail="Ficha no encontrada")
    return item


@router.put("/items/{item_id}")
def update_item(item_id: str, payload: ItemUpdateRequest):
    fields = (
        payload.model_dump(exclude_unset=True)
        if hasattr(payload, "model_dump")
        else payload.dict(exclude_unset=True)
    )
    try:
        item = catalog.update_item(item_id, fields)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return {"ok": True, "item": item}
