from unittest.mock import patch, call

from api.adapter.aws_resource_adapter import AWSResourceAdapter
from api.adapter.dynamodb_adapter import DynamoDBAdapter
from api.application.services.UploadService import UploadService
from api.domain.dataset_filters import DatasetFilters


class TestUploadService:
    upload_service = UploadService()

    @patch.object(AWSResourceAdapter, "get_datasets_metadata")
    @patch.object(DynamoDBAdapter, "get_permissions_for_subject")
    def test_get_authorised_datasets(
        self, mock_get_permissions_for_subject, mock_get_datasets_metadata
    ):
        subject_id = "1234adsfasd8234kj"
        permissions = ["READ_PRIVATE", "WRITE_PUBLIC", "WRITE_PRIVATE"]
        query_public = DatasetFilters(sensitivity="PUBLIC")
        query_private = DatasetFilters(sensitivity="PRIVATE")
        enriched_dataset_metadata_1 = AWSResourceAdapter.EnrichedDatasetMetaData(
            dataset="test_dataset_1", domain="test_domain_1"
        )
        enriched_dataset_metadata_2 = AWSResourceAdapter.EnrichedDatasetMetaData(
            dataset="test_dataset_2", domain="test_domain_2"
        )
        enriched_dataset_metadata_list = [
            enriched_dataset_metadata_1,
            enriched_dataset_metadata_2,
        ]

        mock_get_permissions_for_subject.return_value = permissions
        mock_get_datasets_metadata.return_value = enriched_dataset_metadata_list

        result = self.upload_service.get_authorised_datasets(subject_id)

        assert len(result) == 2
        assert "test_dataset_2" in result
        assert "test_dataset_1" in result
        assert mock_get_datasets_metadata.call_count == 2
        mock_get_permissions_for_subject.assert_called_once_with(subject_id)
        mock_get_datasets_metadata.assert_has_calls(
            [call(query_private), call(query_public)], any_order=True
        )

    @patch.object(AWSResourceAdapter, "get_datasets_metadata")
    @patch.object(DynamoDBAdapter, "get_permissions_for_subject")
    def test_get_authorised_datasets_with_write_all_permission(
        self, mock_get_permissions_for_subject, mock_get_datasets_metadata
    ):
        subject_id = "1234adsfasd8234kj"
        permissions = ["READ_PRIVATE", "WRITE_ALL", "WRITE_PRIVATE"]
        query_public = DatasetFilters(sensitivity="PUBLIC")
        query_private = DatasetFilters(sensitivity="PRIVATE")
        query_protected = DatasetFilters(sensitivity="PROTECTED")
        enriched_dataset_metadata_1 = AWSResourceAdapter.EnrichedDatasetMetaData(
            dataset="test_dataset_1", domain="test_domain_1"
        )
        enriched_dataset_metadata_2 = AWSResourceAdapter.EnrichedDatasetMetaData(
            dataset="test_dataset_2", domain="test_domain_2"
        )
        enriched_dataset_metadata_list = [
            enriched_dataset_metadata_1,
            enriched_dataset_metadata_2,
        ]

        mock_get_permissions_for_subject.return_value = permissions
        mock_get_datasets_metadata.return_value = enriched_dataset_metadata_list

        result = self.upload_service.get_authorised_datasets(subject_id)

        assert len(result) == 2
        assert "test_dataset_2" in result
        assert "test_dataset_1" in result
        assert mock_get_datasets_metadata.call_count == 3
        mock_get_permissions_for_subject.assert_called_once_with(subject_id)
        mock_get_datasets_metadata.assert_has_calls(
            [call(query_private), call(query_public), call(query_protected)],
            any_order=True,
        )

    @patch.object(AWSResourceAdapter, "get_datasets_metadata")
    @patch.object(DynamoDBAdapter, "get_permissions_for_subject")
    def test_get_authorised_datasets_for_write_public(
        self, mock_get_permissions_for_subject, mock_get_datasets_metadata
    ):
        subject_id = "1234adsfasd8234kj"
        permissions = ["READ_ALL", "WRITE_PUBLIC"]
        query_public = DatasetFilters(sensitivity="PUBLIC")
        enriched_dataset_metadata_1 = AWSResourceAdapter.EnrichedDatasetMetaData(
            dataset="test_dataset_1", domain="test_domain_1"
        )
        enriched_dataset_metadata_list = [
            enriched_dataset_metadata_1,
        ]
        mock_get_permissions_for_subject.return_value = permissions
        mock_get_datasets_metadata.return_value = enriched_dataset_metadata_list

        result = self.upload_service.get_authorised_datasets(subject_id)

        assert len(result) == 1
        assert "test_dataset_1" in result
        assert mock_get_datasets_metadata.call_count == 1
        mock_get_permissions_for_subject.assert_called_once_with(subject_id)
        mock_get_datasets_metadata.assert_called_once_with(query_public)

    @patch.object(AWSResourceAdapter, "get_datasets_metadata")
    @patch.object(DynamoDBAdapter, "get_permissions_for_subject")
    def test_get_authorised_datasets_for_protected_domain(
        self, mock_get_permissions_for_subject, mock_get_datasets_metadata
    ):
        subject_id = "1234adsfasd8234kj"
        permissions = ["READ_ALL", "WRITE_PROTECTED_TESTDOMAIN"]
        query_protected = DatasetFilters(sensitivity="PROTECTED")
        enriched_dataset_metadata_1 = AWSResourceAdapter.EnrichedDatasetMetaData(
            dataset="test_dataset_1", domain="TESTDOMAIN"
        )
        enriched_dataset_metadata_2 = AWSResourceAdapter.EnrichedDatasetMetaData(
            dataset="test_dataset_2", domain="ANOTHERDOMAIN"
        )
        enriched_dataset_metadata_list = [
            enriched_dataset_metadata_1,
            enriched_dataset_metadata_2,
        ]
        mock_get_permissions_for_subject.return_value = permissions
        mock_get_datasets_metadata.return_value = enriched_dataset_metadata_list

        result = self.upload_service.get_authorised_datasets(subject_id)

        assert len(result) == 1
        assert "test_dataset_1" in result
        assert mock_get_datasets_metadata.call_count == 1
        mock_get_permissions_for_subject.assert_called_once_with(subject_id)
        mock_get_datasets_metadata.assert_called_once_with(query_protected)
