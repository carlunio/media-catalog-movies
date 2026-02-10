from typing import Any

from pydantic import BaseModel, Field


class RunOmdbRequest(BaseModel):
    movie_id: str | None = None
    limit: int = Field(default=20, ge=1, le=5000)
    overwrite: bool = False


class UpdateOmdbRequest(BaseModel):
    fields: dict[str, Any]
