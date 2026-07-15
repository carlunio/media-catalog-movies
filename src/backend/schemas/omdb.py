from typing import Any

from pydantic import BaseModel, Field


class RunOmdbRequest(BaseModel):
    movie_id: str | None = None
    limit: int = Field(default=20, ge=1, le=5000)
    overwrite: bool = False


class UpdateOmdbRequest(BaseModel):
    fields: dict[str, Any]

class DownloadOmdbCoversRequest(BaseModel):
    ids: list[str] | None = None
    movie_id: str | None = None
    poster_slot: int | None = Field(default=None, ge=1)

