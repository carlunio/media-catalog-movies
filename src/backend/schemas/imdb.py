from pydantic import BaseModel, Field


class RunImdbRequest(BaseModel):
    movie_id: str | None = None
    limit: int = Field(default=20, ge=1, le=5000)
    overwrite: bool = False
    max_results: int = Field(default=3, ge=1, le=20)


class ManualImdbRequest(BaseModel):
    imdb_url: str
