from enum import Enum
from typing import List, Optional, Union

from api.adapter.aws_resource_adapter import AWSResourceAdapter
from api.common.config.layers import Layer
from api.common.custom_exceptions import BaseAppException
from api.domain.dataset_metadata import DatasetMetadata

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


def construct_dataset_metadata(
    layer: Layer, domain: str, dataset: str, version: Optional[int] = None
) -> DatasetMetadata:
    dataset = DatasetMetadata(layer, domain, dataset, version)
    dataset.set_version(aws_resource_adapter)
    return dataset


def build_error_message_list(error: Union[Exception, BaseAppException]) -> List[str]:
    try:
        if isinstance(error.message, list):
            return error.message
        else:
            return [error.message]
    except AttributeError:
        return [str(error)]
