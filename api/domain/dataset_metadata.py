from dataclasses import dataclass
from typing import Optional, TYPE_CHECKING

from api.common.config.aws import DATA_BUCKET, RESOURCE_PREFIX, SCHEMAS_LOCATION
from api.common.config.layers import Layer
from api.common.logger import AppLogger

if TYPE_CHECKING:
    from api.adapter.aws_resource_adapter import AWSResourceAdapter


@dataclass
class DatasetMetadata:
    layer: Layer
    domain: str
    dataset: str
    version: Optional[int] = None

    def dataset_location(self) -> str:
        return f"data/{self.layer}/{self.domain}/{self.dataset}"

    def file_location(self) -> str:
        return f"{self.dataset_location()}/{self.version}"

    def raw_data_location(self) -> str:
        return f"{self.construct_raw_dataset_uploads_location()}/{self.version}"

    def raw_data_path(self, filename: str) -> str:
        return f"{self.raw_data_location()}/{filename}"

    def glue_table_prefix(self):
        return f"{self.layer}_{self.domain}_{self.dataset}_"

    def get_ui_upload_path(self):
        return f"{self.layer}/{self.domain}/{self.dataset}/{self.version}"

    def glue_table_name(self) -> str:
        return f"{self.glue_table_prefix()}{self.version}"

    def s3_path(self) -> str:
        return f"s3://{DATA_BUCKET}/{self.dataset_location()}/"

    def construct_raw_dataset_uploads_location(self):
        return f"raw_data/{self.layer}/{self.domain}/{self.dataset}"

    def construct_schema_dataset_location(self, sensitvity: str):
        return (
            f"{SCHEMAS_LOCATION}/{self.layer}/{sensitvity}/{self.domain}/{self.dataset}"
        )

    def generate_crawler_name(self) -> str:
        return f"{RESOURCE_PREFIX}_crawler/{self.layer}/{self.domain}/{self.dataset}"

    def string_representation(self) -> str:
        if self.version:
            return f"layer [{self.layer}], domain [{self.domain}], dataset [{self.dataset}] and version [{self.version}]"
        else:
            return f"layer [{self.layer}], domain [{self.domain}] and dataset [{self.dataset}]"

    def set_version(self, aws_resource_adapter: "AWSResourceAdapter"):
        if not self.version:
            AppLogger.info(
                "No version provided by the user. Retrieving the latest version from the crawler."
            )
            self.version = aws_resource_adapter.get_version_from_crawler_tags(self)
