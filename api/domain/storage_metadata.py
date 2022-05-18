import time
from dataclasses import dataclass
from typing import Dict, Optional

from api.common.config.aws import DATA_BUCKET


@dataclass(frozen=True)
class StorageMetaData:
    domain: str
    dataset: str

    def location(self) -> str:
        return self._construct_dataset_location(self.domain, self.dataset)

    def raw_data_location(self) -> str:
        return self._construct_raw_dataset_uploads_location(self.domain, self.dataset)

    def raw_data_path(self, filename: str) -> str:
        return f"{self.raw_data_location()}/{filename}"

    def glue_table_prefix(self):
        return self.domain + "_"

    def glue_table_name(self) -> str:
        return f"{self.glue_table_prefix()}{self.dataset}"

    def s3_path(self) -> str:
        return f"s3://{DATA_BUCKET}/{self.location()}/"

    def _construct_dataset_location(self, domain: str, dataset: str):
        return f"data/{domain}/{dataset}"

    def _construct_raw_dataset_uploads_location(self, domain: str, dataset: str):
        return f"raw_data/{domain}/{dataset}"


@dataclass(frozen=True)
class EnrichedDatasetMetaData(StorageMetaData):
    tags: Optional[Dict[str, str]] = None


def filename_with_timestamp(filename: str):
    return f'{time.strftime("%Y-%m-%dT%H:%M:%S")}-{filename}'
