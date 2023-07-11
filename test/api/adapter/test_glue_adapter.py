from unittest.mock import Mock, patch, ANY

import pytest
from botocore.exceptions import ClientError

from api.adapter.glue_adapter import GlueAdapter
from api.common.config.aws import (
    GLUE_TABLE_PRESENCE_CHECK_RETRY_COUNT,
    GLUE_CATALOGUE_DB_NAME,
    DATA_BUCKET,
    AWS_REGION,
    AWS_ACCOUNT,
)
from api.common.config.aws import RESOURCE_PREFIX
from api.common.custom_exceptions import (
    AWSServiceError,
)
from api.domain.dataset_metadata import DatasetMetadata
from api.domain.schema import Column, Schema
from api.domain.schema_metadata import SchemaMetadata


class TestGlueAdapterTableMethods:
    glue_boto_client = None

    def setup_method(self):
        self.glue_boto_client = Mock()
        self.glue_adapter = GlueAdapter(
            self.glue_boto_client,
            "GLUE_CATALOGUE_DB_NAME",
            "GLUE_CRAWLER_ROLE",
            "GLUE_CONNECTION_DB_NAME",
        )

    def test_gets_table_when_created(self):
        table_config = {}
        self.glue_boto_client.get_table.return_value = table_config

        result = self.glue_adapter.get_table_when_created("some-name")

        assert result == table_config

    def test_gets_table_last_updated_date(self):
        table_config = {
            "Table": {
                "Name": "test_e2e",
                "DatabaseName": "rapid_catalogue_db",
                "Owner": "owner",
                "CreateTime": "2022-03-01 11:03:49+00:00",
                "UpdateTime": "2022-03-03 11:03:49+00:00",
                "LastAccessTime": "2022-03-02 11:03:49+00:00",
                "Retention": 0,
            }
        }
        self.glue_boto_client.get_table.return_value = table_config

        result = self.glue_adapter.get_table_last_updated_date("table_name")

        assert result == "2022-03-03 11:03:49+00:00"

    def test_get_no_of_rows_in_table(self):
        table_properties = {
            "Table": {
                "Name": "qa_carsales_25_1",
                "DatabaseName": "rapid_catalogue_db",
                "Owner": "owner",
                "StorageDescriptor": {
                    "Parameters": {
                        "averageRecordSize": "17",
                        "classification": "parquet",
                        "compressionType": "none",
                        "objectCount": "5",
                        "recordCount": "990300",
                        "sizeKey": "4762462",
                        "typeOfData": "file",
                    },
                },
            }
        }
        self.glue_boto_client.get_table.return_value = table_properties

        result = self.glue_adapter.get_no_of_rows("qa_carsales_25_1")

        assert result == 990300

    def test_get_tables_for_dataset(self):
        paginate = self.glue_boto_client.get_paginator.return_value.paginate
        paginate.return_value = [
            {"TableList": [{"Name": "layer_domain_dataset_1"}]},
            {"TableList": [{"Name": "layer_domain_dataset_2"}]},
        ]

        result = self.glue_adapter.get_tables_for_dataset(
            DatasetMetadata("layer", "domain", "dataset")
        )

        assert result == ["layer_domain_dataset_1", "layer_domain_dataset_2"]

    def test_delete_tables(self):
        table_names = ["domain_dataset_1", "domain_dataset_2"]
        self.glue_adapter.delete_tables(table_names)
        self.glue_boto_client.batch_delete_table.assert_called_once_with(
            DatabaseName=GLUE_CATALOGUE_DB_NAME, TablesToDelete=table_names
        )

    def test_delete_tables_fails(self):
        table_names = ["domain_dataset_1", "domain_dataset_2"]
        self.glue_boto_client.batch_delete_table.side_effect = ClientError(
            error_response={"Error": {"Code": "SomethingElse"}},
            operation_name="BatchDeleteTable",
        )

        with pytest.raises(AWSServiceError):
            self.glue_adapter.delete_tables(table_names)

    @patch("api.adapter.glue_adapter.sleep")
    def test_raises_error_when_table_does_not_exist_and_retries_exhausted(
        self, mock_sleep
    ):
        self.glue_boto_client.get_table.side_effect = ClientError(
            error_response={"Error": {"Code": "EntityNotFoundException"}},
            operation_name="GetTable",
        )

        with pytest.raises(
            AWSServiceError, match=r"\[some-name\] was not created after \d+s"
        ):
            self.glue_adapter.get_table_when_created("some-name")

        assert mock_sleep.call_count == GLUE_TABLE_PRESENCE_CHECK_RETRY_COUNT
