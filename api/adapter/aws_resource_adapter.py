from dataclasses import dataclass
from typing import List, Dict, Tuple, Optional, Set

import boto3
from botocore.exceptions import ClientError

from api.common.config.aws import AWS_REGION, RESOURCE_PREFIX
from api.common.custom_exceptions import UserError, AWSServiceError
from api.common.logger import AppLogger
from api.domain.dataset_filters import DatasetFilters
from api.domain.storage_metadata import StorageMetaData


class AWSResourceAdapter:
    def __init__(
        self,
        resource_client=boto3.client(
            "resourcegroupstaggingapi", region_name=AWS_REGION
        ),
    ):
        self.__resource_client = resource_client

    @dataclass(frozen=True)
    class EnrichedDatasetMetaData(StorageMetaData):
        tags: Optional[Dict[str, str]] = None

    def get_datasets_metadata(
        self, query: DatasetFilters = DatasetFilters()
    ) -> List[EnrichedDatasetMetaData]:
        try:
            aws_resources = self._get_resources(
                ["glue:crawler"], query.format_resource_query()
            )
            return self._filter_for_resource_prefix(aws_resources)
        except KeyError:
            return []
        except ClientError as error:
            self._handle_client_error(error)

    def get_existing_domains(self) -> Set[str]:
        datasets_metadata = self.get_datasets_metadata()
        return set([dataset.domain for dataset in datasets_metadata])

    def _filter_for_resource_prefix(self, aws_resources):
        return [
            self._to_dataset_metadata(resource)
            for resource in aws_resources["ResourceTagMappingList"]
            if f":crawler/{RESOURCE_PREFIX}_crawler" in resource["ResourceARN"]
        ]

    def _handle_client_error(self, error):
        AppLogger.error(f"Failed to request datasets tags error={error.response}")
        if (
            error.response
            and error.response["Error"]
            and error.response["Error"]["Code"]
            and error.response["Error"]["Code"] == "InvalidParameterException"
        ):
            raise UserError("Wrong parameters sent to list datasets")
        else:
            raise AWSServiceError(
                "Internal server error, please contact system administrator"
            )

    def _get_resources(self, resource_types: List[str], tag_filters: List[Dict]):
        return self.__resource_client.get_resources(
            ResourceTypeFilters=resource_types, TagFilters=tag_filters
        )

    def _to_dataset_metadata(
        self, resource_tag_mapping: Dict
    ) -> EnrichedDatasetMetaData:
        domain, dataset = self._infer_domain_and_dataset_from_crawler_arn(
            resource_tag_mapping["ResourceARN"]
        )
        tags = {tag["Key"]: tag["Value"] for tag in resource_tag_mapping["Tags"]}
        return self.EnrichedDatasetMetaData(domain, dataset, tags)

    def _infer_domain_and_dataset_from_crawler_arn(self, arn: str) -> Tuple[str, str]:
        table_name = arn.split(f"{RESOURCE_PREFIX}_crawler/")[-1]
        table_name_elements = table_name.split("/")
        return table_name_elements[0], table_name_elements[1]
