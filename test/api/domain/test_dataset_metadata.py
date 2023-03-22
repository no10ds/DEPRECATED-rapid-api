from copy import deepcopy
from unittest.mock import patch, MagicMock, Mock

from api.adapter.aws_resource_adapter import AWSResourceAdapter
from api.common.config.aws import DATA_BUCKET
from api.domain.dataset_metadata import DatasetMetadata


class TestDatasetMetadata:
    def setup_method(self):
        self.dataset_metadata = DatasetMetadata(
            "layer",
            "DOMAIN",
            "DATASET",
            3,
        )

    def test_file_location(self):
        assert self.dataset_metadata.file_location() == "data/layer/DOMAIN/dataset/3"

    def test_dataset_location(self):
        assert self.dataset_metadata.dataset_location() == "data/layer/DOMAIN/dataset"

    def test_raw_data_location(self):
        assert (
            self.dataset_metadata.raw_data_path("filename.csv")
            == "raw_data/layer/DOMAIN/dataset/3/filename.csv"
        )

    def test_glue_table_prefix(self):
        assert self.dataset_metadata.glue_table_prefix() == "layer_DOMAIN_dataset_"

    def test_glue_table_name(self):
        assert self.dataset_metadata.glue_table_name() == "layer_DOMAIN_dataset_3"

    def test_s3_path(self):
        assert (
            self.dataset_metadata.s3_path()
            == f"s3://{DATA_BUCKET}/data/layer/DOMAIN/dataset/"
        )

    def test_construct_dataset_location(self):
        assert self.dataset_metadata.dataset_location() == "data/layer/DOMAIN/dataset"

    def test_construct_raw_dataset_uploads_location(self):
        assert (
            self.dataset_metadata.construct_raw_dataset_uploads_location()
            == "raw_data/layer/DOMAIN/dataset"
        )

    def test_construct_schema_dataset_location(self):
        assert (
            self.dataset_metadata.construct_schema_dataset_location("PROTECTED")
            == "schemas/layer/PROTECTED/DOMAIN/dataset"
        )

    @patch.object(AWSResourceAdapter, "get_version_from_crawler_tags")
    def test_get_latest_version(self, mock_get_version_from_crawler_tags: MagicMock):
        mock_get_version_from_crawler_tags.return_value = 2
        res = self.dataset_metadata.get_latest_version(AWSResourceAdapter())
        assert res == 2

    def test_handle_version_retrieval_when_version_not_present(self):
        dataset_metadata = DatasetMetadata("layer", "domain", "dataset")
        dataset_metadata.get_latest_version = Mock(return_value=11)
        dataset_metadata.handle_version_retrieval(AWSResourceAdapter())
        assert dataset_metadata.version == 11

    def test_handle_version_retrieval_when_version_exists(self):
        original_version = deepcopy(self.dataset_metadata.version)
        self.dataset_metadata.handle_version_retrieval(AWSResourceAdapter())
        assert original_version == self.dataset_metadata.version
