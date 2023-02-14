import re

from api.adapter.glue_adapter import GlueAdapter
from api.adapter.s3_adapter import S3Adapter
from api.common.config.constants import FILENAME_WITH_TIMESTAMP_REGEX
from api.common.custom_exceptions import UserError


class DeleteService:
    def __init__(self, persistence_adapter=S3Adapter(), glue_adapter=GlueAdapter()):
        self.persistence_adapter = persistence_adapter
        self.glue_adapter = glue_adapter

    def delete_schema(self, domain: str, dataset: str, sensitivity: str, version: int):
        self.persistence_adapter.delete_schema(domain, dataset, sensitivity, version)

    def delete_dataset_file(
        self, domain: str, dataset: str, version: int, filename: str
    ):
        self._validate_filename(filename)
        self.persistence_adapter.find_raw_file(domain, dataset, version, filename)
        self.glue_adapter.check_crawler_is_ready(domain, dataset)
        self.persistence_adapter.delete_dataset_files(
            domain, dataset, version, filename
        )
        self.glue_adapter.start_crawler(domain, dataset)

    def delete_dataset(self, domain: str, dataset: str):
        sensitivity = self.persistence_adapter.get_dataset_sensitivity(domain, dataset)
        self.delete_crawler(domain, dataset)
        dataset_files = self.persistence_adapter.list_dataset_files(
            domain, dataset, sensitivity
        )
        self.persistence_adapter.delete_dataset_files_using_key(dataset_files)

        # TODO: Need to delete the glue table
        # The option here is to either delete the glue tables through a boto3 call
        # or change how the crawlers work to move the deprecated system to a delete the table
        # if recrawled over empty data

    def delete_crawler(self, domain: str, dataset: str):
        self.glue_adapter.delete_crawler(domain, dataset)

    def _validate_filename(self, filename: str):
        if not re.match(FILENAME_WITH_TIMESTAMP_REGEX, filename):
            raise UserError(f"Invalid file name [{filename}]")
