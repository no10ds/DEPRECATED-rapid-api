from unittest.mock import Mock

import pytest
from botocore.exceptions import ClientError

from api.adapter.aws_resource_adapter import AWSResourceAdapter
from api.adapter.s3_adapter import S3Adapter
from api.common.config.aws import AWS_REGION, RESOURCE_PREFIX
from api.common.custom_exceptions import UserError, AWSServiceError
from api.domain.dataset_filters import DatasetFilters
from api.domain.dataset_metadata import DatasetMetadata


class TestAWSResourceAdapterClientMethods:
    resource_boto_client = None
    aws_return_value = None

    def setup_method(self):
        self.resource_boto_client = Mock()
        self.mock_s3_client = Mock()
        self.resource_adapter = AWSResourceAdapter(self.resource_boto_client)
        self.s3_adapter = S3Adapter(s3_client=self.mock_s3_client, s3_bucket="dataset")
        self.aws_return_value = [
            {
                "PaginationToken": "xxxx",
                "ResponseMetadata": {"key": "value"},
                "ResourceTagMappingList": [
                    {
                        "ResourceARN": f"arn:aws:glue:{AWS_REGION}:123:crawler/{RESOURCE_PREFIX}_crawler/layer/domain1/dataset1",
                        "Tags": [
                            {"Key": "sensitivity", "Value": "PUBLIC"},
                            {"Key": "no_of_versions", "Value": "1"},
                            {"Key": "layer", "Value": "layer"},
                            {"Key": "domain", "Value": "domain1"},
                        ],
                    },
                    {
                        "ResourceARN": f"arn:aws:glue:{AWS_REGION}:123:crawler/{RESOURCE_PREFIX}_crawler/layer/domain2/dataset2",
                        "Tags": [
                            {"Key": "tag1", "Value": ""},
                            {"Key": "sensitivity", "Value": "PUBLIC"},
                            {"Key": "no_of_versions", "Value": "2"},
                            {"Key": "layer", "Value": "layer"},
                            {"Key": "domain", "Value": "domain2"},
                        ],
                    },
                    {
                        "ResourceARN": f"arn:aws:glue:{AWS_REGION}:123:crawler/{RESOURCE_PREFIX}_crawler/layer/domain3/dataset",
                        "Tags": [
                            {"Key": "tag2", "Value": ""},
                            {"Key": "sensitivity", "Value": "PRIVATE"},
                            {"Key": "no_of_versions", "Value": "3"},
                            {"Key": "layer", "Value": "layer"},
                            {"Key": "domain", "Value": "domain3"},
                        ],
                    },
                ],
            },
            {
                "PaginationToken": "xxxx",
                "ResponseMetadata": {"key": "value"},
                "ResourceTagMappingList": [
                    {
                        "ResourceARN": f"arn:aws:glue:{AWS_REGION}:123:crawler/FAKE_PREFIX_crawler/layer/domain3/dataset3",
                        "Tags": [
                            {"Key": "tag5", "Value": ""},
                            {"Key": "sensitivity", "Value": "PUBLIC"},
                            {"Key": "no_of_versions", "Value": "1"},
                            {"Key": "layer", "Value": "layer"},
                            {"Key": "domain", "Value": "domain3"},
                        ],
                    },
                    {
                        "ResourceARN": f"arn:aws:glue:{AWS_REGION}:123:crawler/{RESOURCE_PREFIX}_crawler/layer/domain3/dataset3",
                        "Tags": [
                            {"Key": "tag5", "Value": ""},
                            {"Key": "sensitivity", "Value": "PUBLIC"},
                            {"Key": "no_of_versions", "Value": "1"},
                            {"Key": "layer", "Value": "layer"},
                            {"Key": "domain", "Value": "domain3"},
                        ],
                    },
                    {
                        "ResourceARN": f"arn:aws:glue:{AWS_REGION}:123:crawler/FAKE_{RESOURCE_PREFIX}_crawler/layer/domain36/dataset3",
                        "Tags": [
                            {"Key": "tag2", "Value": ""},
                            {"Key": "sensitivity", "Value": "PRIVATE"},
                            {"Key": "no_of_versions", "Value": "10"},
                            {"Key": "layer", "Value": "layer"},
                            {"Key": "domain", "Value": "domain36"},
                        ],
                    },
                    {
                        "ResourceARN": f"arn:aws:glue:{AWS_REGION}:123:crawler/{RESOURCE_PREFIX}_crawler/layer/domain36/dataset3",
                        "Tags": [
                            {"Key": "tag2", "Value": ""},
                            {"Key": "sensitivity", "Value": "PRIVATE"},
                            {"Key": "no_of_versions", "Value": "10"},
                            {"Key": "layer", "Value": "layer"},
                            {"Key": "domain", "Value": "domain36"},
                        ],
                    },
                    {
                        "ResourceARN": f"arn:aws:glue:{AWS_REGION}:123:crawler/{RESOURCE_PREFIX}_crawler/raw/domain34/dataset2",
                        "Tags": [
                            {"Key": "tag2", "Value": ""},
                            {"Key": "sensitivity", "Value": "PRIVATE"},
                            {"Key": "no_of_versions", "Value": "10"},
                            {"Key": "layer", "Value": "raw"},
                            {"Key": "domain", "Value": "domain34"},
                        ],
                    },
                ],
            },
        ]

    def test_fetch_resources_from_crawlers(self):
        self.resource_boto_client.get_paginator.return_value.paginate.return_value = (
            self.aws_return_value
        )
        expected = [
            {
                "ResourceARN": f"arn:aws:glue:{AWS_REGION}:123:crawler/{RESOURCE_PREFIX}_crawler/layer/domain1/dataset1",
                "Tags": [
                    {"Key": "sensitivity", "Value": "PUBLIC"},
                    {"Key": "no_of_versions", "Value": "1"},
                    {"Key": "layer", "Value": "layer"},
                    {"Key": "domain", "Value": "domain1"},
                ],
            },
            {
                "ResourceARN": f"arn:aws:glue:{AWS_REGION}:123:crawler/{RESOURCE_PREFIX}_crawler/layer/domain2/dataset2",
                "Tags": [
                    {"Key": "tag1", "Value": ""},
                    {"Key": "sensitivity", "Value": "PUBLIC"},
                    {"Key": "no_of_versions", "Value": "2"},
                    {"Key": "layer", "Value": "layer"},
                    {"Key": "domain", "Value": "domain2"},
                ],
            },
            {
                "ResourceARN": f"arn:aws:glue:{AWS_REGION}:123:crawler/{RESOURCE_PREFIX}_crawler/layer/domain3/dataset",
                "Tags": [
                    {"Key": "tag2", "Value": ""},
                    {"Key": "sensitivity", "Value": "PRIVATE"},
                    {"Key": "no_of_versions", "Value": "3"},
                    {"Key": "layer", "Value": "layer"},
                    {"Key": "domain", "Value": "domain3"},
                ],
            },
            {
                "ResourceARN": f"arn:aws:glue:{AWS_REGION}:123:crawler/{RESOURCE_PREFIX}_crawler/layer/domain3/dataset3",
                "Tags": [
                    {"Key": "tag5", "Value": ""},
                    {"Key": "sensitivity", "Value": "PUBLIC"},
                    {"Key": "no_of_versions", "Value": "1"},
                    {"Key": "layer", "Value": "layer"},
                    {"Key": "domain", "Value": "domain3"},
                ],
            },
            {
                "ResourceARN": f"arn:aws:glue:{AWS_REGION}:123:crawler/{RESOURCE_PREFIX}_crawler/layer/domain36/dataset3",
                "Tags": [
                    {"Key": "tag2", "Value": ""},
                    {"Key": "sensitivity", "Value": "PRIVATE"},
                    {"Key": "no_of_versions", "Value": "10"},
                    {"Key": "layer", "Value": "layer"},
                    {"Key": "domain", "Value": "domain36"},
                ],
            },
            {
                "ResourceARN": f"arn:aws:glue:{AWS_REGION}:123:crawler/{RESOURCE_PREFIX}_crawler/raw/domain34/dataset2",
                "Tags": [
                    {"Key": "tag2", "Value": ""},
                    {"Key": "sensitivity", "Value": "PRIVATE"},
                    {"Key": "no_of_versions", "Value": "10"},
                    {"Key": "layer", "Value": "raw"},
                    {"Key": "domain", "Value": "domain34"},
                ],
            },
        ]

        query = DatasetFilters()
        res = self.resource_adapter.fetch_resources_from_crawlers(query)

        self.resource_boto_client.get_paginator.assert_called_once_with("get_resources")
        self.resource_boto_client.get_paginator.return_value.paginate.assert_called_once_with(
            ResourceTypeFilters=["glue:crawler"],
            TagFilters=[{"Key": "resource_prefix", "Values": [RESOURCE_PREFIX]}],
        )

        assert res == expected

    def test_fetch_resources_from_crawlers_returns_nothing_when_no_datasets_exist(
        self,
    ):
        query = DatasetFilters()

        self.resource_boto_client.get_paginator.return_value.paginate.return_value = {}

        actual_metadatas = self.resource_adapter.fetch_resources_from_crawlers(query)

        self.resource_boto_client.get_paginator.assert_called_once_with("get_resources")
        self.resource_boto_client.get_paginator.return_value.paginate.assert_called_once_with(
            ResourceTypeFilters=["glue:crawler"],
            TagFilters=[{"Key": "resource_prefix", "Values": [RESOURCE_PREFIX]}],
        )

        assert len(actual_metadatas) == 0

    def test_fetch_resources_from_crawlers_calls_resource_client_with_correct_tag_filters(
        self,
    ):
        query = DatasetFilters(
            key_value_tags={
                "tag1": "value1",
                "tag2": None,
            }
        )

        self.resource_boto_client.get_paginator.return_value.paginate.return_value = {}

        self.resource_adapter.fetch_resources_from_crawlers(query)

        self.resource_boto_client.get_paginator.assert_called_once_with("get_resources")
        self.resource_boto_client.get_paginator.return_value.paginate.assert_called_once_with(
            ResourceTypeFilters=["glue:crawler"],
            TagFilters=[
                {"Key": "resource_prefix", "Values": [RESOURCE_PREFIX]},
                {"Key": "tag1", "Values": ["value1"]},
                {"Key": "tag2", "Values": []},
            ],
        )

    def test_get_enriched_datasets_metadata(self):
        self.resource_adapter.fetch_resources_from_crawlers = Mock(
            return_value=[
                {
                    "ResourceARN": f"arn:aws:glue:{AWS_REGION}:123:crawler/{RESOURCE_PREFIX}_crawler/layer/domain1/dataset1",
                    "Tags": [
                        {"Key": "sensitivity", "Value": "PUBLIC"},
                        {"Key": "no_of_versions", "Value": "1"},
                        {"Key": "layer", "Value": "layer"},
                        {"Key": "domain", "Value": "domain1"},
                    ],
                },
                {
                    "ResourceARN": f"arn:aws:glue:{AWS_REGION}:123:crawler/{RESOURCE_PREFIX}_crawler/layer/domain2/dataset2",
                    "Tags": [
                        {"Key": "tag1", "Value": ""},
                        {"Key": "sensitivity", "Value": "PUBLIC"},
                        {"Key": "no_of_versions", "Value": "2"},
                        {"Key": "layer", "Value": "layer"},
                        {"Key": "domain", "Value": "domain2"},
                    ],
                },
            ]
        )
        self.s3_adapter.get_dataset_description = Mock(
            side_effect=["description1", "description2"]
        )

        expected = [
            AWSResourceAdapter.EnrichedDatasetMetaData(
                layer="layer",
                domain="domain1",
                dataset="dataset1",
                version=1,
                description="description1",
                tags={
                    "sensitivity": "PUBLIC",
                    "no_of_versions": "1",
                    "layer": "layer",
                    "domain": "domain1",
                },
            ),
            AWSResourceAdapter.EnrichedDatasetMetaData(
                layer="layer",
                domain="domain2",
                dataset="dataset2",
                version=2,
                description="description2",
                tags={
                    "tag1": "",
                    "sensitivity": "PUBLIC",
                    "no_of_versions": "2",
                    "layer": "layer",
                    "domain": "domain2",
                },
            ),
        ]

        res = self.resource_adapter.get_enriched_datasets_metadata(
            self.s3_adapter, "filters"
        )

        assert expected == res
        self.resource_adapter.fetch_resources_from_crawlers.assert_called_once_with(
            "filters"
        )

    def test_get_enriched_datasets_metadata_returns_invalid_parameter_exception(
        self,
    ):
        query = DatasetFilters(
            tags={
                "tag1": "value1",
                "tag2": None,
            }
        )

        self.resource_boto_client.get_paginator.return_value.paginate.side_effect = (
            ClientError(
                error_response={"Error": {"Code": "InvalidParameterException"}},
                operation_name="GetResources",
            )
        )

        with pytest.raises(UserError, match="Wrong parameters sent to list datasets"):
            self.resource_adapter.get_enriched_datasets_metadata(self.s3_adapter, query)

    @pytest.mark.parametrize(
        "error_code",
        [
            ("ThrottledException",),
            ("InternalServiceException",),
            ("PaginationTokenExpiredException",),
            ("SomethingElse",),
        ],
    )
    def test_get_enriched_datasets_metadata_returns_another_exception(
        self, error_code: str
    ):
        query = DatasetFilters(
            tags={
                "tag1": "value1",
                "tag2": None,
            }
        )

        self.resource_boto_client.get_paginator.return_value.paginate.side_effect = (
            ClientError(
                error_response={"Error": {"Code": f"{error_code}"}},
                operation_name="GetResources",
            )
        )

        with pytest.raises(
            AWSServiceError,
            match="Internal server error, please contact system administrator",
        ):
            self.resource_adapter.get_enriched_datasets_metadata(self.s3_adapter, query)

    def test_get_datasets_metadata(self):
        self.resource_adapter.fetch_resources_from_crawlers = Mock(
            return_value=[
                {
                    "ResourceARN": f"arn:aws:glue:{AWS_REGION}:123:crawler/{RESOURCE_PREFIX}_crawler/layer/domain1/dataset1",
                    "Tags": [
                        {"Key": "sensitivity", "Value": "PUBLIC"},
                        {"Key": "no_of_versions", "Value": "1"},
                        {"Key": "layer", "Value": "layer"},
                        {"Key": "domain", "Value": "domain1"},
                    ],
                },
                {
                    "ResourceARN": f"arn:aws:glue:{AWS_REGION}:123:crawler/{RESOURCE_PREFIX}_crawler/layer/domain2/dataset2",
                    "Tags": [
                        {"Key": "tag1", "Value": ""},
                        {"Key": "sensitivity", "Value": "PUBLIC"},
                        {"Key": "no_of_versions", "Value": "2"},
                        {"Key": "layer", "Value": "layer"},
                        {"Key": "domain", "Value": "domain2"},
                    ],
                },
            ]
        )
        expected = [
            DatasetMetadata(
                layer="layer", domain="domain1", dataset="dataset1", version=1
            ),
            DatasetMetadata(
                layer="layer", domain="domain2", dataset="dataset2", version=2
            ),
        ]

        res = self.resource_adapter.get_datasets_metadata("filters")

        assert expected == res
        self.resource_adapter.fetch_resources_from_crawlers.assert_called_once_with(
            "filters"
        )

    def test_get_datasets_metadata_returns_invalid_parameter_exception(
        self,
    ):
        query = DatasetFilters(
            tags={
                "tag1": "value1",
                "tag2": None,
            }
        )

        self.resource_boto_client.get_paginator.return_value.paginate.side_effect = (
            ClientError(
                error_response={"Error": {"Code": "InvalidParameterException"}},
                operation_name="GetResources",
            )
        )

        with pytest.raises(UserError, match="Wrong parameters sent to list datasets"):
            self.resource_adapter.get_datasets_metadata(query)

    @pytest.mark.parametrize(
        "error_code",
        [
            ("ThrottledException",),
            ("InternalServiceException",),
            ("PaginationTokenExpiredException",),
            ("SomethingElse",),
        ],
    )
    def test_get_datasets_metadata_returns_another_exception(self, error_code: str):
        query = DatasetFilters(
            tags={
                "tag1": "value1",
                "tag2": None,
            }
        )

        self.resource_boto_client.get_paginator.return_value.paginate.side_effect = (
            ClientError(
                error_response={"Error": {"Code": f"{error_code}"}},
                operation_name="GetResources",
            )
        )

        with pytest.raises(
            AWSServiceError,
            match="Internal server error, please contact system administrator",
        ):
            self.resource_adapter.get_datasets_metadata(query)

    @pytest.mark.parametrize(
        "layer, domain, dataset, expected_version",
        [
            ("layer", "domain1", "dataset1", 1),
            ("layer", "domain2", "dataset2", 2),
            ("layer", "domain3", "dataset", 3),
            ("layer", "domain3", "dataset3", 1),
            ("layer", "domain36", "dataset3", 10),
            ("raw", "domain34", "dataset2", 10),
        ],
    )
    def test_get_version_from_crawler(
        self, layer: str, domain: str, dataset: str, expected_version: int
    ):
        self.resource_boto_client.get_paginator.return_value.paginate.return_value = (
            self.aws_return_value
        )

        actual_version = self.resource_adapter.get_version_from_crawler_tags(
            DatasetMetadata(layer, domain, dataset)
        )

        assert actual_version == expected_version
