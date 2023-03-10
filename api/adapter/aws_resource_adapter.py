from dataclasses import dataclass
from typing import TYPE_CHECKING, Dict, List, Optional

import boto3
from botocore.exceptions import ClientError

from api.common.config.aws import AWS_REGION, RESOURCE_PREFIX
from api.common.config.constants import FIRST_SCHEMA_VERSION_NUMBER
from api.common.custom_exceptions import AWSServiceError, UserError
from api.common.logger import AppLogger
from api.domain.dataset_filters import DatasetFilters
from api.domain.dataset_metadata import DatasetMetadata

if TYPE_CHECKING:
    from api.adapter.s3_adapter import S3Adapter


class AWSResourceAdapter:
    def __init__(
        self,
        resource_client=boto3.client(
            "resourcegroupstaggingapi", region_name=AWS_REGION
        ),
    ):
        self.__resource_client = resource_client

    @dataclass
    class EnrichedDatasetMetaData(DatasetMetadata):
        description: Optional[str] = ""
        tags: Optional[Dict[str, str]] = None

    def get_datasets_metadata(
        self, s3_adapter: "S3Adapter", query: DatasetFilters = DatasetFilters()
    ) -> List[EnrichedDatasetMetaData]:
        try:
            AppLogger.info("Getting datasets info")
            aws_resources = self._get_resources(
                ["glue:crawler"], query.format_resource_query()
            )
            resources_prefix = self._filter_for_resource_prefix(aws_resources)
            return [
                self._to_dataset_metadata(resource, s3_adapter)
                for resource in resources_prefix
            ]
        except KeyError:
            return []
        except ClientError as error:
            self._handle_client_error(error)

    def _filter_for_resource_prefix(self, aws_resources):
        return [
            resource
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
        AppLogger.info(f"Getting AWS resources with tags {tag_filters}")
        return self.__resource_client.get_resources(
            ResourceTypeFilters=resource_types, TagFilters=tag_filters
        )

    def _to_dataset_metadata(
        self, resource_tag_mapping: Dict, s3_adapter: "S3Adapter"
    ) -> EnrichedDatasetMetaData:
        dataset = self._infer_dataset_metadata_from_crawler_arn(
            resource_tag_mapping["ResourceARN"]
        )
        tags = {tag["Key"]: tag["Value"] for tag in resource_tag_mapping["Tags"]}
        dataset.version = self.get_version_from_tags(resource_tag_mapping)
        description = s3_adapter.get_dataset_description(dataset)
        return self.EnrichedDatasetMetaData(
            dataset.layer,
            dataset.domain,
            dataset.dataset,
            dataset.version,
            description,
            tags,
        )

    def get_version_from_tags(self, resource_tag_mapping):
        version_tag = [
            tag["Value"]
            for tag in resource_tag_mapping["Tags"]
            if tag["Key"] == "no_of_versions"
        ]
        return int(version_tag[0]) if version_tag else FIRST_SCHEMA_VERSION_NUMBER

    def get_version_from_crawler_tags(self, dataset: DatasetMetadata) -> int:
        aws_resources = self._get_resources(["glue:crawler"], [])

        crawler_resource = None

        AppLogger.info(
            f"Getting version for layer {dataset.layer} domain {dataset.domain} and dataset {dataset.dataset}"
        )
        for resource in aws_resources["ResourceTagMappingList"]:
            if resource["ResourceARN"].endswith(dataset.generate_crawler_name()):
                crawler_resource = resource

        return self.get_version_from_tags(crawler_resource)

    def _infer_dataset_metadata_from_crawler_arn(self, arn: str) -> DatasetMetadata:
        table_name = arn.split(f"{RESOURCE_PREFIX}_crawler/")[-1]
        table_name_elements = table_name.split("/")
        return DatasetMetadata(
            table_name_elements[0], table_name_elements[1], table_name_elements[2]
        )
