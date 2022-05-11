from unittest.mock import patch

import pandas as pd
import pytest

from api.adapter.athena_adapter import DatasetQuery
from api.adapter.aws_resource_adapter import AWSResourceAdapter
from api.application.services.data_service import DataService
from api.application.services.delete_service import DeleteService
from api.common.custom_exceptions import UserError, DatasetError, CrawlerStartFailsError, \
    SchemaNotFoundError, CrawlerIsNotReadyError, GetCrawlerError
from api.domain.dataset_filter_query import DatasetFilterQuery
from api.domain.schema import Schema, SchemaMetadata, Owner, Column
from api.domain.sql_query import SQLQuery
from api.domain.storage_metadata import EnrichedDatasetMetaData

from test.api.controller.controller_test_utils import BaseClientTest


class TestDataUpload(BaseClientTest):

    @patch.object(DataService, "upload_dataset")
    def test_calls_data_upload_service_successfully(self, mock_upload_dataset):
        file_content = b"some,content"
        file_name = "filename.csv"
        file_name_with_timestamp = f"2022-05-05T12:00:00-{file_name}"

        mock_upload_dataset.return_value = file_name_with_timestamp

        response = self.client.post(
            "/datasets/domain/dataset",
            files={"file": (file_name, file_content, "text/csv")},
            headers={"Authorization": "Bearer test-token"}
        )

        mock_upload_dataset.assert_called_once_with("domain", "dataset", file_name, file_content)

        assert response.status_code == 201
        assert response.json() == {"uploaded": file_name_with_timestamp}

    @patch.object(DataService, "upload_dataset")
    def test_calls_data_upload_service_fails_when_invalid_dataset_is_uploaded(self, mock_upload_dataset):
        file_content = b"some,content"
        file_name = "filename.csv"

        mock_upload_dataset.side_effect = DatasetError("Expected 3 columns, received 4")

        response = self.client.post(
            "/datasets/domain/dataset",
            files={"file": (file_name, file_content, "text/csv")},
            headers={"Authorization": "Bearer test-token"}
        )

        mock_upload_dataset.assert_called_once_with("domain", "dataset", file_name, file_content)

        assert response.status_code == 400
        assert response.json() == {"details": "Expected 3 columns, received 4"}

    @patch.object(DataService, "upload_dataset")
    def test_succeeds_to_upload_data_and_fails_to_start_crawler(self, mock_upload_dataset):
        file_content = b"some,content"
        file_name = "filename.csv"

        mock_upload_dataset.side_effect = CrawlerStartFailsError("Failed to start crawler")

        response = self.client.post(
            "/datasets/domain/dataset",
            files={"file": (file_name, file_content, "text/csv")},
            headers={"Authorization": "Bearer test-token"}
        )

        mock_upload_dataset.assert_called_once_with("domain", "dataset", file_name, file_content)

        assert response.status_code == 202
        assert response.json() == {"uploaded": file_name}

    def test_calls_data_fails_with_missing_path(self):
        file_content = b"some,content"
        file_name = "filename.csv"

        response = self.client.post(
            "/datasets//dataset", files={"file": (file_name, file_content, "text/csv")},
            headers={"Authorization": "Bearer test-token"}
        )

        assert response.status_code == 404

    def test_raises_validation_error_when_file_not_provided(self):
        response = self.client.post("/datasets/domain/dataset", headers={"Authorization": "Bearer test-token"})
        assert response.status_code == 400

    @patch.object(DataService, "upload_dataset")
    def test_raises_error_when_schema_does_not_exist(self, mock_upload_dataset):
        file_content = b"some,content"
        file_name = "filename.csv"

        mock_upload_dataset.side_effect = SchemaNotFoundError("Error message")

        response = self.client.post(
            "/datasets/domain/dataset",
            files={"file": (file_name, file_content, "text/csv")},
            headers={"Authorization": "Bearer test-token"}
        )

        assert response.status_code == 400

    @patch.object(DataService, "upload_dataset")
    def test_raises_error_when_crawler_is_already_running(self, mock_upload_dataset):
        file_content = b"some,content"
        file_name = "filename.csv"

        mock_upload_dataset.side_effect = CrawlerIsNotReadyError("Some message")

        response = self.client.post(
            "/datasets/domain/dataset",
            files={"file": (file_name, file_content, "text/csv")},
            headers={"Authorization": "Bearer test-token"}
        )

        mock_upload_dataset.assert_called_once_with("domain", "dataset", file_name, file_content)

        assert response.status_code == 429
        assert response.json() == {'details': 'Data is currently processing. Please try again later.'}

    @patch.object(DataService, "upload_dataset")
    def test_raises_error_when_fails_to_get_crawler_state(self, mock_upload_dataset):
        file_content = b"some,content"
        file_name = "filename.csv"

        mock_upload_dataset.side_effect = GetCrawlerError("Some message")

        response = self.client.post(
            "/datasets/domain/dataset",
            files={"file": (file_name, file_content, "text/csv")},
            headers={"Authorization": "Bearer test-token"}
        )

        mock_upload_dataset.assert_called_once_with("domain", "dataset", file_name, file_content)

        assert response.status_code == 500
        assert response.json() == {'details': 'Internal failure when uploading data.'}


class TestListDatasets(BaseClientTest):

    @patch.object(AWSResourceAdapter, "get_datasets_metadata")
    def test_returns_metadata_for_all_datasets(self, mock_get_datasets_metadata):
        metadata_response = [
            EnrichedDatasetMetaData(domain="domain1", dataset="dataset1", tags={"tag1": "value1"}),
            EnrichedDatasetMetaData(domain="domain2", dataset="dataset2", tags={"tag2": "value2"}),
        ]

        mock_get_datasets_metadata.return_value = metadata_response

        expected_response = [
            {
                "domain": "domain1",
                "dataset": "dataset1",
                "tags": {
                    "tag1": "value1"
                }
            },
            {
                "domain": "domain2",
                "dataset": "dataset2",
                "tags": {
                    "tag2": "value2"
                }
            }
        ]

        expected_query = DatasetFilterQuery()

        response = self.client.post(
            "/datasets",
            headers={"Authorization": "Bearer test-token"},
            # Not passing a JSON body here to filter by tags
        )

        mock_get_datasets_metadata.assert_called_once_with(expected_query)

        assert response.status_code == 200
        assert response.json() == expected_response

    @patch.object(AWSResourceAdapter, "get_datasets_metadata")
    def test_returns_metadata_for_datasets_with_certain_tags(self, mock_get_datasets_metadata):
        metadata_response = [
            EnrichedDatasetMetaData(domain="domain1", dataset="dataset1", tags={"tag1": "value1"}),
            EnrichedDatasetMetaData(domain="domain2", dataset="dataset2", tags={"tag2": "value2"}),
        ]

        mock_get_datasets_metadata.return_value = metadata_response

        expected_response = [
            {
                "domain": "domain1",
                "dataset": "dataset1",
                "tags": {
                    "tag1": "value1"
                }
            },
            {
                "domain": "domain2",
                "dataset": "dataset2",
                "tags": {
                    "tag2": "value2"
                }
            }
        ]

        tag_filters = {
            "tag1": "value1",
            "tag2": "",
        }

        expected_query_object = DatasetFilterQuery(sensitivity=None, tags=tag_filters)

        response = self.client.post(
            "/datasets",
            headers={"Authorization": "Bearer test-token"},
            json={"tags": tag_filters}
        )

        mock_get_datasets_metadata.assert_called_once_with(expected_query_object)

        assert response.status_code == 200
        assert response.json() == expected_response

    @patch.object(AWSResourceAdapter, "get_datasets_metadata")
    def test_returns_metadata_for_datasets_with_certain_sensitivity(self, mock_get_datasets_metadata):
        metadata_response = [
            EnrichedDatasetMetaData(domain="domain1", dataset="dataset1",
                                    tags={"sensitivity": "PUBLIC", "tag1": "value1"}),
            EnrichedDatasetMetaData(domain="domain2", dataset="dataset2", tags={"sensitivity": "PUBLIC"}),
        ]

        mock_get_datasets_metadata.return_value = metadata_response

        expected_response = [
            {
                "domain": "domain1",
                "dataset": "dataset1",
                "tags": {
                    "sensitivity": "PUBLIC",
                    "tag1": "value1"
                }
            },
            {
                "domain": "domain2",
                "dataset": "dataset2",
                "tags": {
                    "sensitivity": "PUBLIC"
                }
            }
        ]

        expected_query_object = DatasetFilterQuery(sensitivity="PUBLIC")

        response = self.client.post(
            "/datasets",
            headers={"Authorization": "Bearer test-token"},
            json={
                "sensitivity": "PUBLIC"
            }
        )

        mock_get_datasets_metadata.assert_called_once_with(expected_query_object)

        assert response.status_code == 200
        assert response.json() == expected_response


class TestDatasetInfo(BaseClientTest):

    @patch.object(DataService, "get_dataset_info")
    def test_returns_metadata_for_all_datasets(self, mock_get_dataset_info):
        expected_response = Schema(
            metadata=SchemaMetadata(domain="mydomain", dataset="mydataset", sensitivity="PUBLIC",
                                    owners=[Owner(name="owner", email="owner@email.com")]),
            columns=[
                Column(name="colname1", partition_index=None, data_type="object", allow_null=True, format=None),
                Column(name="colname2", partition_index=None, data_type="Int64", allow_null=True, format=None)
            ]
        )
        mock_get_dataset_info.return_value = expected_response

        response = self.client.get(
            "/datasets/mydomain/mydataset/info",
            headers={"Authorization": "Bearer test-token"},
            # Not passing a JSON body here to filter by tags
        )

        mock_get_dataset_info.assert_called_once_with("mydomain", "mydataset")

        assert response.status_code == 200
        assert response.json() == expected_response


class TestQuery(BaseClientTest):

    @patch.object(DatasetQuery, "query")
    def test_call_service_with_only_domain_dataset_when_no_json_provided(self, mock_query_method):
        query_url = "/datasets/mydomain/mydataset/query"

        self.client.post(query_url, headers={"Authorization": "Bearer test-token"})

        mock_query_method.assert_called_once_with(
            "mydomain",
            "mydataset",
            SQLQuery()
        )

    @patch.object(DatasetQuery, "query")
    def test_call_service_with_sql_query_when_json_provided(self, mock_query_method):
        request_json = {
            "select_columns": ["column1"],
            "limit": "10"
        }

        query_url = "/datasets/mydomain/mydataset/query"

        self.client.post(
            query_url,
            headers={"Authorization": "Bearer test-token"},
            json=request_json
        )

        mock_query_method.assert_called_once_with(
            "mydomain", "mydataset",
            SQLQuery(select_columns=['column1'], limit="10")
        )

    @patch.object(DatasetQuery, "query")
    def test_calls_service_with_sql_query_when_empty_json_values_provided(self, mock_query_method):
        request_json = {
            "select_columns": ["column1"],
            "filter": "",
            "aggregation_conditions": "",
            "limit": "10"
        }

        query_url = "/datasets/mydomain/mydataset/query"

        self.client.post(
            query_url,
            headers={"Authorization": "Bearer test-token"},
            json=request_json
        )

        mock_query_method.assert_called_once_with(
            "mydomain", "mydataset",
            SQLQuery(select_columns=['column1'], filter="", aggregation_conditions="", limit="10")
        )

    @patch.object(DatasetQuery, "query")
    def test_returns_formatted_json_from_query_result(self, mock_query_method):
        mock_query_method.return_value = pd.DataFrame({
            "column1": [1, 2],
            "column2": ["item1", "item2"],
            "area": ["area_1", "area_2"],
        })

        query_url = "/datasets/mydomain/mydataset/query"

        response = self.client.post(
            query_url,
            headers={
                "Authorization": "Bearer test-token",
                "Accept": "application/json"
            })

        assert response.status_code == 200

        assert response.json() == {
            "0": {"column1": "1", "column2": "item1", "area": "area_1"},
            "1": {"column1": "2", "column2": "item2", "area": "area_2"},
        }

    @patch.object(DatasetQuery, "query")
    def test_request_query_in_csv_is_successful(self, mock_query_method):
        mock_query_method.return_value = pd.DataFrame({
            "column1": [1, 2],
            "column2": ["item1", "item2"],
            "area": ["area_1", "area_2"],
        })

        query_url = "/datasets/mydomain/mydataset/query"

        response = self.client.post(
            query_url,
            headers={
                "Authorization": "Bearer test-token",
                "Accept": "text/csv"})

        assert response.status_code == 200

    @patch.object(DatasetQuery, "query")
    def test_returns_formatted_json_from_query_if_format_is_not_provided(self, mock_query_method):
        mock_query_method.return_value = pd.DataFrame({
            "column1": [1, 2],
            "column2": ["item1", "item2"],
            "area": ["area_1", "area_2"],
        })

        query_url = "/datasets/mydomain/mydataset/query"

        response = self.client.post(
            query_url,
            headers={
                "Authorization": "Bearer test-token"
            })

        assert response.status_code == 200
        assert response.json() == {
            "0": {"column1": "1", "column2": "item1", "area": "area_1"},
            "1": {"column1": "2", "column2": "item2", "area": "area_2"},
        }

    @patch.object(DatasetQuery, "query")
    def test_returns_error_from_query_request_when_format_is_unsupported(self, mock_query_method):
        mock_query_method.return_value = pd.DataFrame({
            "column1": [1, 2],
            "column2": ["item1", "item2"],
            "area": ["area_1", "area_2"],
        })

        query_url = "/datasets/mydomain/mydataset/query"

        response = self.client.post(
            query_url,
            headers={
                "Authorization": "Bearer test-token",
                "Accept": "text/plain"})

        assert response.status_code == 400
        assert response.json() == {
            "details": "Provided value for Accept header parameter [text/plain] is not supported. Supported formats: application/json, text/csv"
        }

    @pytest.mark.parametrize("input_key", [
        "select_column",
        "invalid_key",
        "another_invalid_key"])
    def test_returns_error_from_query_request_when_invalid_key(self, input_key: str):
        query_url = "/datasets/mydomain/mydataset/query"

        response = self.client.post(
            query_url,
            headers={
                "Authorization": "Bearer test-token",
                "Accept": "text/csv"},
            json={input_key: "some_value"}
        )

        assert response.status_code == 400
        assert response.json() == {
            "details": [f"{input_key} -> extra fields not permitted"]
        }


class TestListFilesFromDataset(BaseClientTest):
    @patch.object(DataService, "list_raw_files")
    def test_returns_metadata_for_all_datasets(self, mock_list_raw_files):
        mock_list_raw_files.return_value = [
            "2020-01-01T12:00:00-file1.csv",
            "2020-07-01T16:00:00-file2.csv",
            "2020-11-01T15:00:00-file3.csv",
        ]

        response = self.client.get(
            "/datasets/mydomain/mydataset/files",
            headers={"Authorization": "Bearer test-token"})

        mock_list_raw_files.assert_called_once_with("mydomain", "mydataset")

        assert response.status_code == 200


class TestDeleteFiles(BaseClientTest):
    @patch.object(DeleteService, "delete_dataset_file")
    def test_returns_204_when_file_is_deleted(self, mock_delete_dataset_file):
        response = self.client.delete(
            "/datasets/mydomain/mydataset/2022-01-01T00:00:00-file.csv",
            headers={"Authorization": "Bearer test-token"})

        mock_delete_dataset_file.assert_called_once_with("mydomain", "mydataset", "2022-01-01T00:00:00-file.csv")

        assert response.status_code == 204

    @patch.object(DeleteService, "delete_dataset_file")
    def test_returns_429_when_crawler_is_not_ready_before_deletion(self, mock_delete_dataset_file):
        mock_delete_dataset_file.side_effect = CrawlerIsNotReadyError('Some random message')

        response = self.client.delete("/datasets/mydomain/mydataset/2022-01-01T00:00:00-file.csv",
                                      headers={"Authorization": "Bearer test-token"})

        mock_delete_dataset_file.assert_called_once_with("mydomain", "mydataset", "2022-01-01T00:00:00-file.csv")

        assert response.status_code == 429
        assert response.json() == {"details": "Unable to delete file. Please try again later."}

    @patch.object(DeleteService, "delete_dataset_file")
    def test_returns_202_when_crawler_cannot_start_after_deletion(self, mock_delete_dataset_file):
        mock_delete_dataset_file.side_effect = CrawlerStartFailsError('Some random message')

        response = self.client.delete("/datasets/mydomain/mydataset/2022-01-01T00:00:00-file.csv",
                                      headers={"Authorization": "Bearer test-token"})

        mock_delete_dataset_file.assert_called_once_with("mydomain", "mydataset", "2022-01-01T00:00:00-file.csv")

        assert response.status_code == 202
        assert response.json() == ["2022-01-01T00:00:00-file.csv has been deleted."]

    @patch.object(DeleteService, "delete_dataset_file")
    def test_returns_400_when_file_name_does_not_exist(self, mock_delete_dataset_file):
        mock_delete_dataset_file.side_effect = UserError('Some random message')

        response = self.client.delete("/datasets/mydomain/mydataset/2022-01-01T00:00:00-file.csv",
                                      headers={"Authorization": "Bearer test-token"})

        mock_delete_dataset_file.assert_called_once_with("mydomain", "mydataset", "2022-01-01T00:00:00-file.csv")

        assert response.status_code == 400
        assert response.json() == {'details': 'Some random message'}
