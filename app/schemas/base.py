from pydantic import (
    BaseModel,
    ConfigDict
)


class Base(BaseModel):
    model_config = ConfigDict(
        extra='ignore'
    )


__all__ = ['Base']
