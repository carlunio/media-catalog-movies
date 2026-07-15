from pydantic import BaseModel, Field


class IngestRequest(BaseModel):
    folder: str
    recursive: bool = True
    extensions: list[str] | None = None
    overwrite_existing_paths: bool = False


class RunExtractRequest(BaseModel):
    movie_id: str | None = None
    limit: int = Field(default=20, ge=1, le=5000)
    overwrite: bool = False
    title_model: str | None = None
    team_model: str | None = None
