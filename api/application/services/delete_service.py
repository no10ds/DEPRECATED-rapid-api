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
        domain = domain.lower()
        self._validate_filename(filename)
        self.persistence_adapter.find_raw_file(domain, dataset, version, filename)
        self.glue_adapter.check_crawler_is_ready(domain, dataset)
        self.persistence_adapter.delete_dataset_files(
            domain, dataset, version, filename
        )
        self.glue_adapter.start_crawler(domain, dataset)

    def _validate_filename(self, filename: str):
        if not re.match(FILENAME_WITH_TIMESTAMP_REGEX, filename):
            raise UserError(f"Invalid file name [{filename}]")
