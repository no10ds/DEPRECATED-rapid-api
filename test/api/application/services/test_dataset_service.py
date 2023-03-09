from unittest.mock import patch, MagicMock, call

from api.adapter.aws_resource_adapter import AWSResourceAdapter
from api.adapter.dynamodb_adapter import DynamoDBAdapter
from api.application.services.dataset_service import DatasetService
from api.common.config.auth import Action
from api.domain.dataset_filters import DatasetFilters


def verify_get_datasets_metadata_call_list(
    mock_get_datasets_metadata, s3_adapter, expected_filters
):
    for _filter in expected_filters:
        assert call(s3_adapter, _filter) in mock_get_datasets_metadata.call_args_list
    assert mock_get_datasets_metadata.call_count == len(expected_filters)


class TestWriteDatasets:
    s3_adapter = MagicMock()
    upload_service = DatasetService(s3_adapter=s3_adapter)

    @patch.object(AWSResourceAdapter, "get_datasets_metadata")
    @patch.object(DynamoDBAdapter, "get_permissions_for_subject")
    def test_get_authorised_datasets(
        self, mock_get_permissions_for_subject, mock_get_datasets_metadata
    ):
        action = Action.WRITE
        subject_id = "1234adsfasd8234kj"
        permissions = ["READ_ALL_PRIVATE", "WRITE_ALL_PUBLIC", "WRITE_ALL_PRIVATE"]
        enriched_dataset_metadata_1 = AWSResourceAdapter.EnrichedDatasetMetaData(
            layer="raw", dataset="test_dataset_1", domain="test_domain_1", version=1
        )
        enriched_dataset_metadata_2 = AWSResourceAdapter.EnrichedDatasetMetaData(
            layer="raw", dataset="test_dataset_2", domain="test_domain_2", version=2
        )
        expected_filters = [
            DatasetFilters(sensitivity="PUBLIC"),
            DatasetFilters(sensitivity="PRIVATE"),
        ]

        mock_get_permissions_for_subject.return_value = permissions
        mock_get_datasets_metadata.return_value = [
            enriched_dataset_metadata_1,
            enriched_dataset_metadata_2,
        ]

        result = self.upload_service.get_authorised_datasets(subject_id, action)

        assert result == [
            "raw/test_domain_1/test_dataset_1/1",
            "raw/test_domain_2/test_dataset_2/2",
        ]
        verify_get_datasets_metadata_call_list(
            mock_get_datasets_metadata, self.s3_adapter, expected_filters
        )

        mock_get_permissions_for_subject.assert_called_once_with(subject_id)

    @patch.object(AWSResourceAdapter, "get_datasets_metadata")
    @patch.object(DynamoDBAdapter, "get_permissions_for_subject")
    def test_get_authorised_datasets_with_write_all_permission(
        self, mock_get_permissions_for_subject, mock_get_datasets_metadata
    ):
        action = Action.WRITE
        subject_id = "1234adsfasd8234kj"
        permissions = ["READ_ALL_PRIVATE", "WRITE_ALL", "WRITE_ALL_PRIVATE"]
        enriched_dataset_metadata_1 = AWSResourceAdapter.EnrichedDatasetMetaData(
            layer="raw",
            dataset="test_public_dataset",
            domain="test_domain_1",
            version=1,
        )
        enriched_dataset_metadata_2 = AWSResourceAdapter.EnrichedDatasetMetaData(
            layer="raw",
            dataset="test_private_dataset",
            domain="test_domain_2",
            version=2,
        )
        enriched_dataset_metadata_3 = AWSResourceAdapter.EnrichedDatasetMetaData(
            layer="raw",
            dataset="test_protected_dataset",
            domain="test_domain_3",
            version=3,
        )
        expected_filters = [
            DatasetFilters(sensitivity="PUBLIC"),
            DatasetFilters(sensitivity="PRIVATE"),
            DatasetFilters(sensitivity="PROTECTED"),
        ]
        mock_get_permissions_for_subject.return_value = permissions
        mock_get_datasets_metadata.side_effect = [
            [enriched_dataset_metadata_1],
            [enriched_dataset_metadata_2],
            [enriched_dataset_metadata_3],
        ]

        result = self.upload_service.get_authorised_datasets(subject_id, action)

        assert result == [
            "raw/test_domain_1/test_public_dataset/1",
            "raw/test_domain_2/test_private_dataset/2",
            "raw/test_domain_3/test_protected_dataset/3",
        ]
        verify_get_datasets_metadata_call_list(
            mock_get_datasets_metadata, self.s3_adapter, expected_filters
        )
        mock_get_permissions_for_subject.assert_called_once_with(subject_id)

    @patch.object(AWSResourceAdapter, "get_datasets_metadata")
    @patch.object(DynamoDBAdapter, "get_permissions_for_subject")
    def test_get_authorised_datasets_for_write_public(
        self, mock_get_permissions_for_subject, mock_get_datasets_metadata
    ):
        action = Action.WRITE
        subject_id = "1234adsfasd8234kj"
        permissions = ["READ_ALL", "WRITE_ALL_PUBLIC"]
        enriched_dataset_metadata_1 = AWSResourceAdapter.EnrichedDatasetMetaData(
            layer="raw", dataset="test_dataset_1", domain="test_domain_1", version=3
        )
        expected_filters = [DatasetFilters(sensitivity="PUBLIC")]
        mock_get_permissions_for_subject.return_value = permissions
        mock_get_datasets_metadata.side_effect = [
            [enriched_dataset_metadata_1],
        ]
        result = self.upload_service.get_authorised_datasets(subject_id, action)

        assert result == ["raw/test_domain_1/test_dataset_1/3"]
        verify_get_datasets_metadata_call_list(
            mock_get_datasets_metadata, self.s3_adapter, expected_filters
        )
        mock_get_permissions_for_subject.assert_called_once_with(subject_id)

    @patch.object(AWSResourceAdapter, "get_datasets_metadata")
    @patch.object(DynamoDBAdapter, "get_permissions_for_subject")
    def test_get_authorised_datasets_for_write_protected_domain(
        self, mock_get_permissions_for_subject, mock_get_datasets_metadata
    ):
        action = Action.WRITE
        subject_id = "1234adsfasd8234kj"
        permissions = [
            "READ_ALL",
            "WRITE_ALL_PUBLIC",
            "WRITE_ALL_PROTECTED_TEST2DOMAIN",
        ]
        enriched_dataset_metadata_1 = AWSResourceAdapter.EnrichedDatasetMetaData(
            layer="raw", dataset="test_dataset_1", domain="some_domain", version=1
        )
        enriched_dataset_metadata_protected_domain = (
            AWSResourceAdapter.EnrichedDatasetMetaData(
                layer="raw", dataset="test_dataset_2", domain="test2domain", version=1
            )
        )
        expected_filters = [
            DatasetFilters(domain="test2domain", sensitivity="PROTECTED"),
            DatasetFilters(sensitivity="PUBLIC"),
        ]

        mock_get_permissions_for_subject.return_value = permissions
        mock_get_datasets_metadata.side_effect = [
            [enriched_dataset_metadata_1],
            [enriched_dataset_metadata_protected_domain],
        ]

        result = self.upload_service.get_authorised_datasets(subject_id, action)

        assert result == [
            "raw/some_domain/test_dataset_1/1",
            "raw/test2domain/test_dataset_2/1",
        ]
        verify_get_datasets_metadata_call_list(
            mock_get_datasets_metadata, self.s3_adapter, expected_filters
        )
        mock_get_permissions_for_subject.assert_called_once_with(subject_id)

    @patch.object(AWSResourceAdapter, "get_datasets_metadata")
    @patch.object(DynamoDBAdapter, "get_permissions_for_subject")
    def test_get_authorised_datasets_for_write_protected_domain_and_specific_layer(
        self,
        mock_get_permissions_for_subject: MagicMock,
        mock_get_datasets_metadata: MagicMock,
    ):
        action = Action.WRITE
        subject_id = "1234adsfasd8234kj"
        permissions = [
            "READ_ALL",
            "WRITE_RAW_PUBLIC",
            "WRITE_RAW_PROTECTED_TEST2DOMAIN",
        ]
        enriched_dataset_metadata_1 = AWSResourceAdapter.EnrichedDatasetMetaData(
            layer="raw", dataset="test_dataset_1", domain="some_domain", version=1
        )
        enriched_dataset_metadata_protected_domain = (
            AWSResourceAdapter.EnrichedDatasetMetaData(
                layer="raw", dataset="test_dataset_2", domain="test2domain", version=1
            )
        )

        expected_filters = [
            DatasetFilters(layer="RAW", domain="test2domain", sensitivity="PROTECTED"),
            DatasetFilters(layer="RAW", sensitivity="PUBLIC"),
        ]

        mock_get_permissions_for_subject.return_value = permissions
        mock_get_datasets_metadata.side_effect = [
            [enriched_dataset_metadata_1],
            [enriched_dataset_metadata_protected_domain],
        ]

        result = self.upload_service.get_authorised_datasets(subject_id, action)

        assert result == [
            "raw/some_domain/test_dataset_1/1",
            "raw/test2domain/test_dataset_2/1",
        ]
        verify_get_datasets_metadata_call_list(
            mock_get_datasets_metadata, self.s3_adapter, expected_filters
        )
        mock_get_permissions_for_subject.assert_called_once_with(subject_id)


class TestReadDatasets:
    s3_adapter = MagicMock()
    upload_service = DatasetService(s3_adapter=s3_adapter)

    @patch.object(AWSResourceAdapter, "get_datasets_metadata")
    @patch.object(DynamoDBAdapter, "get_permissions_for_subject")
    def test_get_authorised_datasets(
        self, mock_get_permissions_for_subject, mock_get_datasets_metadata
    ):
        action = Action.READ
        subject_id = "1234adsfasd8234kj"
        permissions = ["READ_ALL_PRIVATE", "READ_ALL_PUBLIC", "WRITE_ALL_PRIVATE"]
        enriched_dataset_metadata_1 = AWSResourceAdapter.EnrichedDatasetMetaData(
            layer="raw", dataset="test_dataset_1", domain="test_domain_1", version=1
        )
        enriched_dataset_metadata_2 = AWSResourceAdapter.EnrichedDatasetMetaData(
            layer="raw", dataset="test_dataset_2", domain="test_domain_2", version=2
        )
        expected_filters = [
            DatasetFilters(sensitivity="PRIVATE"),
            DatasetFilters(sensitivity="PUBLIC"),
        ]

        mock_get_permissions_for_subject.return_value = permissions
        mock_get_datasets_metadata.side_effect = [
            [enriched_dataset_metadata_1],
            [enriched_dataset_metadata_2],
        ]

        result = self.upload_service.get_authorised_datasets(subject_id, action)

        assert result == [
            "raw/test_domain_1/test_dataset_1/1",
            "raw/test_domain_2/test_dataset_2/2",
        ]
        verify_get_datasets_metadata_call_list(
            mock_get_datasets_metadata, self.s3_adapter, expected_filters
        )
        mock_get_permissions_for_subject.assert_called_once_with(subject_id)

    @patch.object(AWSResourceAdapter, "get_datasets_metadata")
    @patch.object(DynamoDBAdapter, "get_permissions_for_subject")
    def test_get_authorised_datasets_with_read_all_permission(
        self, mock_get_permissions_for_subject, mock_get_datasets_metadata
    ):
        action = Action.READ
        subject_id = "1234adsfasd8234kj"
        permissions = ["READ_ALL_PRIVATE", "READ_ALL", "WRITE_ALL_PRIVATE"]

        enriched_dataset_metadata_1 = AWSResourceAdapter.EnrichedDatasetMetaData(
            layer="raw",
            dataset="test_public_dataset",
            domain="test_domain_1",
            version=2,
        )
        enriched_dataset_metadata_2 = AWSResourceAdapter.EnrichedDatasetMetaData(
            layer="raw",
            dataset="test_private_dataset",
            domain="test_domain_2",
            version=2,
        )
        enriched_dataset_metadata_3 = AWSResourceAdapter.EnrichedDatasetMetaData(
            layer="raw",
            dataset="test_protected_dataset",
            domain="test_domain_3",
            version=2,
        )

        expected_filters = [
            DatasetFilters(sensitivity="PRIVATE"),
            DatasetFilters(sensitivity="PROTECTED"),
            DatasetFilters(sensitivity="PUBLIC"),
        ]

        mock_get_permissions_for_subject.return_value = permissions
        mock_get_datasets_metadata.side_effect = [
            [enriched_dataset_metadata_1],
            [enriched_dataset_metadata_2],
            [enriched_dataset_metadata_3],
        ]

        result = self.upload_service.get_authorised_datasets(subject_id, action)

        assert result == [
            "raw/test_domain_1/test_public_dataset/2",
            "raw/test_domain_2/test_private_dataset/2",
            "raw/test_domain_3/test_protected_dataset/2",
        ]
        verify_get_datasets_metadata_call_list(
            mock_get_datasets_metadata, self.s3_adapter, expected_filters
        )
        mock_get_permissions_for_subject.assert_called_once_with(subject_id)

    @patch.object(AWSResourceAdapter, "get_datasets_metadata")
    @patch.object(DynamoDBAdapter, "get_permissions_for_subject")
    def test_get_authorised_datasets_for_read_public(
        self, mock_get_permissions_for_subject, mock_get_datasets_metadata
    ):
        action = Action.READ
        subject_id = "1234adsfasd8234kj"
        permissions = ["WRITE_ALL", "READ_ALL_PUBLIC"]
        enriched_dataset_metadata_1 = AWSResourceAdapter.EnrichedDatasetMetaData(
            layer="raw", dataset="test_dataset_1", domain="test_domain_1", version=100
        )
        expected_filters = [
            DatasetFilters(sensitivity="PUBLIC"),
        ]

        enriched_dataset_metadata_list = [
            enriched_dataset_metadata_1,
        ]
        mock_get_permissions_for_subject.return_value = permissions
        mock_get_datasets_metadata.return_value = enriched_dataset_metadata_list

        result = self.upload_service.get_authorised_datasets(subject_id, action)

        assert result == ["raw/test_domain_1/test_dataset_1/100"]
        verify_get_datasets_metadata_call_list(
            mock_get_datasets_metadata, self.s3_adapter, expected_filters
        )
        mock_get_permissions_for_subject.assert_called_once_with(subject_id)

    @patch.object(AWSResourceAdapter, "get_datasets_metadata")
    @patch.object(DynamoDBAdapter, "get_permissions_for_subject")
    def test_get_authorised_datasets_for_read_protected_domain(
        self, mock_get_permissions_for_subject, mock_get_datasets_metadata
    ):
        action = Action.READ
        subject_id = "1234adsfasd8234kj"
        permissions = ["WRITE_ALL", "READ_ALL_PUBLIC", "READ_ALL_PROTECTED_TEST2DOMAIN"]
        enriched_dataset_metadata_1 = AWSResourceAdapter.EnrichedDatasetMetaData(
            layer="raw", dataset="test_dataset_1", domain="some_domain", version=2
        )
        enriched_dataset_metadata_protected_domain = (
            AWSResourceAdapter.EnrichedDatasetMetaData(
                layer="raw", dataset="test_dataset_2", domain="test2domain", version=1
            )
        )
        expected_filters = [
            DatasetFilters(domain="test2domain", sensitivity="PROTECTED"),
            DatasetFilters(sensitivity="PUBLIC"),
        ]
        mock_get_permissions_for_subject.return_value = permissions
        mock_get_datasets_metadata.side_effect = [
            [enriched_dataset_metadata_1],
            [enriched_dataset_metadata_protected_domain],
        ]

        result = self.upload_service.get_authorised_datasets(subject_id, action)

        assert result == [
            "raw/some_domain/test_dataset_1/2",
            "raw/test2domain/test_dataset_2/1",
        ]
        verify_get_datasets_metadata_call_list(
            mock_get_datasets_metadata, self.s3_adapter, expected_filters
        )
        mock_get_permissions_for_subject.assert_called_once_with(subject_id)

    @patch.object(AWSResourceAdapter, "get_datasets_metadata")
    @patch.object(DynamoDBAdapter, "get_permissions_for_subject")
    def test_get_authorised_datasets_for_read_protected_domain_and_specific_layer(
        self, mock_get_permissions_for_subject, mock_get_datasets_metadata
    ):
        action = Action.READ
        subject_id = "1234adsfasd8234kj"
        permissions = ["WRITE_RAW", "READ_RAW_PUBLIC", "READ_RAW_PROTECTED_TEST2DOMAIN"]
        enriched_dataset_metadata_1 = AWSResourceAdapter.EnrichedDatasetMetaData(
            layer="raw", dataset="test_dataset_1", domain="some_domain", version=2
        )
        enriched_dataset_metadata_protected_domain = (
            AWSResourceAdapter.EnrichedDatasetMetaData(
                layer="raw", dataset="test_dataset_2", domain="test2domain", version=1
            )
        )
        expected_filters = [
            DatasetFilters(layer="RAW", domain="test2domain", sensitivity="PROTECTED"),
            DatasetFilters(layer="RAW", sensitivity="PUBLIC"),
        ]
        mock_get_permissions_for_subject.return_value = permissions
        mock_get_datasets_metadata.side_effect = [
            [enriched_dataset_metadata_1],
            [enriched_dataset_metadata_protected_domain],
        ]

        result = self.upload_service.get_authorised_datasets(subject_id, action)
        assert result == [
            "raw/some_domain/test_dataset_1/2",
            "raw/test2domain/test_dataset_2/1",
        ]
        verify_get_datasets_metadata_call_list(
            mock_get_datasets_metadata, self.s3_adapter, expected_filters
        )
        mock_get_permissions_for_subject.assert_called_once_with(subject_id)
