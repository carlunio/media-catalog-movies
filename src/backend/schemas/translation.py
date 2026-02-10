from pydantic import BaseModel, Field


class RunTranslationRequest(BaseModel):
    movie_id: str | None = None
    limit: int = Field(default=20, ge=1, le=5000)
    overwrite: bool = False
    model: str | None = None


class UpdatePlotTranslationRequest(BaseModel):
    plot_es: str
