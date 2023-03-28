import re

from api.adapter.glue_adapter import GlueAdapter
from api.adapter.s3_adapter import S3Adapter
from api.common.config.constants import FILENAME_WITH_TIMESTAMP_REGEX
from api.common.custom_exceptions import UserError
from api.domain.dataset_metadata import DatasetMetadata
from api.domain.schema_metadata import SchemaMetadata


class DeleteService:
    def __init__(self, persistence_adapter=S3Adapter(), glue_adapter=GlueAdapter()):
        self.persistence_adapter = persistence_adapter
        self.glue_adapter = glue_adapter

    def delete_schema(self, schema_metadata: SchemaMetadata):
        self.persistence_adapter.delete_schema(schema_metadata)

    def delete_dataset_file(self, dataset: DatasetMetadata, filename: str):
        self._validate_filename(filename)
        self.persistence_adapter.find_raw_file(dataset, filename)
        self.glue_adapter.check_crawler_is_ready(dataset)
        self.persistence_adapter.delete_dataset_files(dataset, filename)
        self.glue_adapter.start_crawler(dataset)

    def delete_dataset(self, dataset: DatasetMetadata):
        # Given a domain and a dataset, delete all rAPId contents for this domain & dataset
        # 1. Generate a list of file keys from S3 to delete, raw_data, data & schemas
        # 2. Remove keys
        # 3. Delete Glue Tables
        sensitivity = self.persistence_adapter.get_dataset_sensitivity(
            dataset.layer, dataset.domain, dataset.dataset
        )
        dataset_files = self.persistence_adapter.list_dataset_files(
            dataset, sensitivity
        )
        # 4. Delete crawler
        self.persistence_adapter.delete_dataset_files_using_key(
            dataset_files, f"{dataset.layer}/{dataset.domain}/{dataset.dataset}"
        )
        tables = self.glue_adapter.get_tables_for_dataset(dataset)
        self.glue_adapter.delete_tables(tables)
        self.glue_adapter.delete_crawler(dataset)

    def _validate_filename(self, filename: str):
        if not re.match(FILENAME_WITH_TIMESTAMP_REGEX, filename):
            raise UserError(f"Invalid file name [{filename}]")
