from pydantic import Field

from .base import Base


class FileFilterParams(Base):
    keep_latest: bool = Field(default=False)
    include: list[str] | None = Field(default=None)
    exclude: list[str] | None = Field(default=None)
    key: str | None = Field(default=None)
    skip: int | None = Field(default=None)
