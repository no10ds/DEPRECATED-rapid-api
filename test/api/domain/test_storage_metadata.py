import re

from api.common.config.aws import DATA_BUCKET
from api.domain.storage_metadata import StorageMetaData, filename_with_timestamp


class TestStorageMetaData:
    def setup_method(self):
        self.dataset_meta_data = StorageMetaData("DOMAIN", "DATASET")

    def test_location(self):
        assert self.dataset_meta_data.location() == "data/DOMAIN/DATASET"

    def test_raw_data_location(self):
        assert (
            self.dataset_meta_data.raw_data_path("filename.csv")
            == "raw_data/DOMAIN/DATASET/filename.csv"
        )

    def test_glue_table_prefix(self):
        assert self.dataset_meta_data.glue_table_prefix() == "DOMAIN_"

    def test_glue_table_name(self):
        assert self.dataset_meta_data.glue_table_name() == "DOMAIN_DATASET"

    def test_s3_path(self):
        assert (
            self.dataset_meta_data.s3_path()
            == f"s3://{DATA_BUCKET}/data/DOMAIN/DATASET/"
        )


def test_filename_with_timestamp():
    filename = filename_with_timestamp("my_data.csv")
    pattern = "\\d{4}-\\d{2}-\\d{2}T\\d{2}\\:\\d{2}\\:\\d{2}-my_data.csv"
    assert re.match(pattern, filename)
