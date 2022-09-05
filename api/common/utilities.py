from enum import Enum

from api.adapter.aws_resource_adapter import AWSResourceAdapter

aws_resource_adapter = AWSResourceAdapter()


class BaseEnum(Enum):
    @classmethod
    def values(cls):
        return [item.value for item in cls]

    @classmethod
    def from_string(cls, value: str):
        if value not in cls.values():
            raise ValueError(f"{value} is not an accepted value")
        return cls(value)


def handle_version_retrieval(domain, dataset, version) -> int:
    if not version:
        version = aws_resource_adapter.get_version_from_crawler_tags(domain, dataset)
    return version
