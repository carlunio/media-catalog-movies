from typing import Literal

from pydantic import BaseModel, Field

WorkflowStage = Literal["extraction", "imdb", "omdb", "translation"]
WorkflowReviewAction = Literal[
    "approve",
    "retry_from_extraction",
    "retry_from_imdb",
    "retry_from_omdb",
    "retry_from_translation",
]


class WorkflowRunRequest(BaseModel):
    movie_id: str | None = None
    limit: int = Field(default=20, ge=1, le=5000)
    start_stage: WorkflowStage = "extraction"
    stop_after: WorkflowStage | None = None
    action: str | None = None
    overwrite: bool = False

    title_model: str | None = None
    team_model: str | None = None
    translation_model: str | None = None

    max_results: int = Field(default=10, ge=1, le=30)
    max_attempts: int | None = Field(default=None, ge=0, le=20)


class WorkflowReviewRequest(BaseModel):
    action: WorkflowReviewAction
    max_attempts: int | None = Field(default=None, ge=0, le=20)

    title_model: str | None = None
    team_model: str | None = None
    translation_model: str | None = None

    max_results: int = Field(default=10, ge=1, le=30)


class WorkflowMarkReviewRequest(BaseModel):
    reason: str | None = None
    node: str = "manual"
