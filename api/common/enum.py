from enum import Enum


class BaseEnum(Enum):
    @classmethod
    def values(cls):
        return [item.value for item in cls]

    @classmethod
    def from_string(cls, value: str):
        if value not in cls.values():
            raise ValueError(f"{value} is not an accepted value")
        return cls(value)
