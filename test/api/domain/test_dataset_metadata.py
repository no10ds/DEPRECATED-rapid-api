from copy import deepcopy
from unittest.mock import Mock

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
        assert self.dataset_metadata.file_location() == "data/layer/DOMAIN/DATASET/3"

    def test_dataset_location(self):
        assert self.dataset_metadata.dataset_location() == "data/layer/DOMAIN/DATASET"

    def test_raw_data_location(self):
        assert (
            self.dataset_metadata.raw_data_path("filename.csv")
            == "raw_data/layer/DOMAIN/DATASET/3/filename.csv"
        )

    def test_glue_table_prefix(self):
        assert self.dataset_metadata.glue_table_prefix() == "layer_DOMAIN_DATASET_"

    def test_glue_table_name(self):
        assert self.dataset_metadata.glue_table_name() == "layer_DOMAIN_DATASET_3"

    def test_s3_path(self):
        assert (
            self.dataset_metadata.s3_path()
            == f"s3://{DATA_BUCKET}/data/layer/DOMAIN/DATASET/"
        )

    def test_construct_dataset_location(self):
        assert self.dataset_metadata.dataset_location() == "data/layer/DOMAIN/DATASET"

    def test_construct_raw_dataset_uploads_location(self):
        assert (
            self.dataset_metadata.construct_raw_dataset_uploads_location()
            == "raw_data/layer/DOMAIN/DATASET"
        )

    def test_construct_schema_dataset_location(self):
        assert (
            self.dataset_metadata.construct_schema_dataset_location("PROTECTED")
            == "schemas/layer/PROTECTED/DOMAIN/DATASET"
        )

    def test_set_version_when_version_not_present(self):
        dataset_metadata = DatasetMetadata("layer", "domain", "dataset")
        aws_resource_adapter = AWSResourceAdapter()
        aws_resource_adapter.get_version_from_crawler_tags = Mock(return_value=11)
        dataset_metadata.set_version(aws_resource_adapter)
        assert dataset_metadata.version == 11

    def test_set_version_when_version_exists(self):
        original_version = deepcopy(self.dataset_metadata.version)
        self.dataset_metadata.set_version(AWSResourceAdapter())
        assert original_version == self.dataset_metadata.version
