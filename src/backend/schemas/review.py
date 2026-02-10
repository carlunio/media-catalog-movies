from pydantic import BaseModel, Field


class UpdateTitleTeamRequest(BaseModel):
    title: str | None = None
    team: list[str] = Field(default_factory=list)
