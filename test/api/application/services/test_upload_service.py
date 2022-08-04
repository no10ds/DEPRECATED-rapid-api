from unittest.mock import patch

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
        # Handle ALL and protected domains
        permissions = ["READ_PRIVATE", "WRITE_PUBLIC"]
        query = DatasetFilters(sensitivity="PUBLIC")
        enriched_dataset_metadata = AWSResourceAdapter.EnrichedDatasetMetaData(
            dataset="test_dataset", domain="test_domain"
        )
        enriched_dataset_metadata_list = [enriched_dataset_metadata]

        mock_get_permissions_for_subject.return_value = permissions
        mock_get_datasets_metadata.return_value = enriched_dataset_metadata_list

        result = self.upload_service.get_authorised_datasets(subject_id)

        assert len(result) == 1
        assert result.pop() == "test_dataset"
        mock_get_permissions_for_subject.assert_called_once_with(subject_id)
        mock_get_datasets_metadata.assert_called_once_with(query)
