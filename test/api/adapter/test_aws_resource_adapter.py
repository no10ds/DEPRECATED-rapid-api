from unittest.mock import Mock

import pytest
from botocore.exceptions import ClientError

from api.adapter.aws_resource_adapter import AWSResourceAdapter
from api.common.config.aws import AWS_REGION
from api.common.custom_exceptions import UserError, AWSServiceError
from api.domain.dataset_filters import DatasetFilters


class TestAWSResourceAdapterClientMethods:
    resource_boto_client = None
    aws_return_value = None

    def setup_method(self):
        self.resource_boto_client = Mock()
        self.resource_adapter = AWSResourceAdapter(self.resource_boto_client)
        self.aws_return_value = {
            "ResourceTagMappingList": [
                {
                    "ResourceARN": f"arn:aws:glue:{AWS_REGION}:123:crawler/rapid_crawler/domain1/dataset1",
                    "Tags": [{"Key": "sensitivity", "Value": "PUBLIC"}],
                },
                {
                    "ResourceARN": f"arn:aws:glue:{AWS_REGION}:123:crawler/rapid_crawler/domain2/dataset2",
                    "Tags": [
                        {"Key": "tag1", "Value": ""},
                        {"Key": "sensitivity", "Value": "PUBLIC"},
                    ],
                },
            ]
        }

    def test_gets_all_datasets_metadata_when_query_is_empty(self):
        query = DatasetFilters()

        expected_metadatas = [
            AWSResourceAdapter.EnrichedDatasetMetaData(
                domain="domain1", dataset="dataset1", tags={"sensitivity": "PUBLIC"}
            ),
            AWSResourceAdapter.EnrichedDatasetMetaData(
                domain="domain2",
                dataset="dataset2",
                tags={"tag1": "", "sensitivity": "PUBLIC"},
            ),
        ]

        self.resource_boto_client.get_resources.return_value = self.aws_return_value

        actual_metadatas = self.resource_adapter.get_datasets_metadata(query)

        self.resource_boto_client.get_resources.assert_called_once_with(
            ResourceTypeFilters=["glue:crawler"], TagFilters=[]
        )
        assert actual_metadatas == expected_metadatas

    def test_returns_empty_list_of_datasets_when_none_exist(self):
        query = DatasetFilters()

        self.resource_boto_client.get_resources.return_value = {}

        actual_metadatas = self.resource_adapter.get_datasets_metadata(query)

        self.resource_boto_client.get_resources.assert_called_once_with(
            ResourceTypeFilters=["glue:crawler"], TagFilters=[]
        )

        assert len(actual_metadatas) == 0

    def test_calls_resource_client_with_correct_tag_filters(self):
        query = DatasetFilters(
            key_value_tags={
                "tag1": "value1",
                "tag2": None,
            }
        )

        self.resource_boto_client.get_resources.return_value = {}

        self.resource_adapter.get_datasets_metadata(query)

        self.resource_boto_client.get_resources.assert_called_once_with(
            ResourceTypeFilters=["glue:crawler"],
            TagFilters=[
                {"Key": "tag1", "Values": ["value1"]},
                {"Key": "tag2", "Values": []},
            ],
        )

    def test_resource_client_returns_invalid_parameter_exception(self):
        query = DatasetFilters(
            tags={
                "tag1": "value1",
                "tag2": None,
            }
        )

        self.resource_boto_client.get_resources.side_effect = ClientError(
            error_response={"Error": {"Code": "InvalidParameterException"}},
            operation_name="GetResources",
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
    def test_resource_client_returns_another_exception(self, error_code: str):
        query = DatasetFilters(
            tags={
                "tag1": "value1",
                "tag2": None,
            }
        )

        self.resource_boto_client.get_resources.side_effect = ClientError(
            error_response={"Error": {"Code": f"{error_code}"}},
            operation_name="GetResources",
        )

        with pytest.raises(
            AWSServiceError,
            match="Internal server error, please contact system administrator",
        ):
            self.resource_adapter.get_datasets_metadata(query)
