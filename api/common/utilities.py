from typing import List, Optional, Union

from api.adapter.aws_resource_adapter import AWSResourceAdapter
from api.common.config.layers import Layer
from api.common.custom_exceptions import BaseAppException
from api.domain.dataset_metadata import DatasetMetadata

aws_resource_adapter = AWSResourceAdapter()


def construct_dataset_metadata(
    layer: Layer, domain: str, dataset: str, version: Optional[int] = None
) -> DatasetMetadata:
    dataset = DatasetMetadata(layer, domain, dataset, version)
    dataset.handle_version_retrieval(aws_resource_adapter)

    return dataset


def build_error_message_list(error: Union[Exception, BaseAppException]) -> List[str]:
    try:
        if isinstance(error.message, list):
            return error.message
        else:
            return [error.message]
    except AttributeError:
        return [str(error)]
