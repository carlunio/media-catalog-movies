from pydantic import BaseModel


class ItemUpdateRequest(BaseModel):
    title: str | None = None
    original_title: str | None = None
    item_type: str | None = None
    director: str | None = None
    writers: str | None = None
    actors: str | None = None
    year: str | None = None
    rated: str | None = None
    released: str | None = None
    runtime: str | None = None
    genres: str | None = None
    country: str | None = None
    languages: str | None = None
    plot: str | None = None
    awards: str | None = None
    production: str | None = None
    imdb_url: str | None = None
    imdb_rating: str | None = None
    imdb_votes: str | None = None
    box_office: str | None = None
    sale_price: float | None = None
    listing_status: str | None = None
    stock_status: str | None = None
    tc_section: str | int | None = None
    tc_condition: str | None = None
    condition_comments: str | None = None
    notes: str | None = None
    image_path: str | None = None


class ExportItemsRequest(BaseModel):
    ids: list[str]
