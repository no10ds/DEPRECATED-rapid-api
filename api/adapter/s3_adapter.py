import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

import boto3
import pandas as pd
from botocore.exceptions import ClientError
from botocore.response import StreamingBody

from api.common.config.auth import SensitivityLevel
from api.common.config.aws import (
    AWS_REGION,
    DATA_BUCKET,
    OUTPUT_QUERY_BUCKET,
    SCHEMAS_LOCATION,
)
from api.common.config.constants import (
    CONTENT_ENCODING,
    QUERY_RESULTS_LINK_EXPIRY_SECONDS,
)
from api.common.config.layers import Layer
from api.common.custom_exceptions import AWSServiceError, SchemaNotFoundError, UserError
from api.common.logger import AppLogger
from api.domain.dataset_metadata import DatasetMetadata
from api.domain.schema import Schema
from api.domain.schema_metadata import SchemaMetadata, SchemaMetadatas


class S3Adapter:
    def __init__(
        self,
        s3_client=boto3.client(
            "s3",
            region_name=AWS_REGION,
            config=boto3.session.Config(signature_version="s3v4"),
        ),
        s3_bucket=DATA_BUCKET,
    ):
        self.__s3_client = s3_client
        self.__s3_bucket = s3_bucket

    def store_data(self, object_full_path: str, object_content: bytes):
        self._validate_file(object_content, object_full_path)

        self.__s3_client.put_object(
            Bucket=self.__s3_bucket, Key=object_full_path, Body=object_content
        )

    def retrieve_data(self, key: str) -> StreamingBody:
        response: Dict = self.__s3_client.get_object(Bucket=self.__s3_bucket, Key=key)
        return response.get("Body")

    def find_schema(self, dataset: DatasetMetadata) -> Optional[Schema]:
        try:
            schema_metadata = self.retrieve_schema_metadata(dataset)
            dataset = self.retrieve_data(schema_metadata.schema_path())
            return Schema.parse_raw(dataset.read())
        except SchemaNotFoundError:
            return None
        except ClientError as error:
            if error.response["Error"]["Code"] == "NoSuchKey":
                return None

    def find_raw_file(self, dataset: DatasetMetadata, filename: str):
        try:
            self.retrieve_data(dataset.raw_data_path(filename))
        except ClientError as error:
            if error.response["Error"]["Code"] == "NoSuchKey":
                raise UserError(f"The file [{filename}] does not exist")

    def save_schema(self, schema: Schema) -> str:
        schema.metadata.domain = schema.metadata.domain.lower()
        schema_metadata = schema.metadata
        self.store_data(
            object_full_path=schema_metadata.schema_path(),
            object_content=self._convert_to_bytes(schema.json()),
        )
        return schema_metadata.schema_name()

    def delete_schema(self, schema_metadata: SchemaMetadata):
        self._delete_data(schema_metadata.schema_path())

    def get_dataset_sensitivity(
        self, layer: Optional[Layer], domain: Optional[str], dataset: Optional[str]
    ) -> SensitivityLevel:
        if any(not arg for arg in [layer, domain, dataset]):
            return SensitivityLevel.from_string("PUBLIC")
        # all datasets have the same sensitivity - take the first version
        schema_metadata = self.retrieve_schema_metadata(
            DatasetMetadata(layer, domain, dataset, version=1)
        )
        return SensitivityLevel.from_string(schema_metadata.get_sensitivity())

    def get_dataset_description(self, dataset: DatasetMetadata) -> str:
        schema = self.retrieve_schema_metadata(dataset)
        return schema.get_description()

    def upload_partitioned_data(
        self,
        dataset: DatasetMetadata,
        filename: str,
        partitioned_data: List[Tuple[str, pd.DataFrame]],
    ):
        for index, (partition_path, data) in enumerate(partitioned_data):
            upload_path = self._construct_partitioned_data_path(
                partition_path, filename, dataset
            )
            data_content = data.to_parquet(compression="gzip", index=False)
            self.store_data(upload_path, data_content)

    def upload_raw_data(
        self, schema_metadata: SchemaMetadata, file_path: Path, raw_file_identifier: str
    ):
        domain = schema_metadata.get_domain()
        dataset = schema_metadata.get_dataset()
        version = schema_metadata.get_version()
        layer = schema_metadata.get_layer()
        dataset_metadata = schema_metadata.get_dataset_metadata()
        AppLogger.info(
            f"Raw data upload for {dataset_metadata.raw_data_location()} started"
        )
        filename = f"{raw_file_identifier}.csv"
        raw_data_path = schema_metadata.get_dataset_metadata().raw_data_path(filename)
        self.__s3_client.upload_file(
            Filename=file_path.name, Bucket=self.__s3_bucket, Key=raw_data_path
        )
        AppLogger.info(
            f"Raw data upload for {layer}/{domain}/{dataset}/{version} completed"
        )

    def list_raw_files(self, dataset: DatasetMetadata) -> List[str]:
        object_list = self._list_files_from_path(dataset.raw_data_location())
        return self._map_object_list_to_filename(object_list)

    def list_dataset_files(
        self, dataset: DatasetMetadata, sensitivity: SensitivityLevel
    ) -> List[Dict]:

        return [
            *self._list_files_from_path(
                dataset.construct_raw_dataset_uploads_location()
            ),
            *self._list_files_from_path(dataset.dataset_location()),
            *self._list_files_from_path(
                dataset.construct_schema_dataset_location(sensitivity)
            ),
        ]

    def delete_dataset_files(
        self, dataset: DatasetMetadata, raw_data_filename: str
    ) -> None:
        files = self._list_files_from_path(dataset.file_location())
        raw_file_identifier = self._clean_filename(raw_data_filename)

        files_to_delete = [
            {"Key": data_file["Key"]}
            for data_file in files
            if self._clean_filename(data_file["Key"]).startswith(raw_file_identifier)
        ]

        self._delete_objects(files_to_delete, raw_data_filename)

    def delete_dataset_files_using_key(self, keys: List[Dict], filename: str):
        files_to_delete = [{"Key": key["Key"]} for key in keys]
        self._delete_objects(files_to_delete, filename)

    def delete_raw_dataset_files(
        self,
        dataset: DatasetMetadata,
        raw_data_filename: str,
    ):
        files_to_delete = [{"Key": dataset.raw_data_path(raw_data_filename)}]

        self._delete_objects(files_to_delete, raw_data_filename)

    def generate_query_result_download_url(self, query_execution_id: str) -> str:
        try:
            return self.__s3_client.generate_presigned_url(
                ClientMethod="get_object",
                Params={
                    "Bucket": OUTPUT_QUERY_BUCKET,
                    "Key": f"{query_execution_id}.csv",
                },
                HttpMethod="GET",
                ExpiresIn=QUERY_RESULTS_LINK_EXPIRY_SECONDS,
            )
        except ClientError as error:
            AppLogger.error(
                f"Unable to generate pre-signed URL for execution ID {query_execution_id}, {error}"
            )
            raise AWSServiceError("Unable to generate download URL")

    def retrieve_schema_metadata(self, dataset: DatasetMetadata) -> SchemaMetadata:
        schemas = self._list_all_schemas()
        return schemas.find(dataset)

    def _clean_filename(self, file_key: str) -> str:
        return file_key.rsplit("/", 1)[-1].split(".")[0]

    def _construct_partitioned_data_path(
        self, partition_path: str, filename: str, dataset: DatasetMetadata
    ) -> str:
        return os.path.join(dataset.file_location(), partition_path, filename)

    def _delete_objects(self, files_to_delete: List[Dict], filename: str):
        response = self.__s3_client.delete_objects(
            Bucket=self.__s3_bucket, Delete={"Objects": files_to_delete}
        )
        self._handle_deletion_response(filename, response)

    def _handle_deletion_response(self, filename, response):
        if "Deleted" in response:
            AppLogger.info(
                f'Files deleted: {[item["Key"] for item in response["Deleted"]]}'
            )
        if "Errors" in response:
            message = "\n".join([str(error) for error in response["Errors"]])
            AppLogger.error(f"Error during file deletion [{filename}]: \n{message}")
            raise AWSServiceError(
                f"The item [{filename}] could not be deleted. Please contact your administrator."
            )

    def _list_files_from_path(self, file_path: str) -> List[Dict]:
        try:
            response = self.__s3_client.list_objects(
                Bucket=self.__s3_bucket,
                Prefix=file_path,
            )
            return response["Contents"]
        except KeyError:
            return []

    def _map_object_list_to_filename(self, object_list) -> List[str]:
        if len(object_list) > 0:
            return [
                self._extract_filename(item["Key"])
                for item in object_list
                if item["Key"].endswith(".csv")
            ]
        return object_list

    def _extract_filename(self, item: str) -> str:
        return item.rsplit("/", 1)[-1]

    def _convert_to_bytes(self, data: str):
        return bytes(data.encode(CONTENT_ENCODING))

    def _validate_file(self, object_content, object_full_path):
        if not self._valid_object_name(object_full_path):
            raise UserError("File path is invalid")
        if not self._valid_object_content(object_content):
            raise UserError("File content is invalid")

    def _valid_object_name(self, object_name: str) -> bool:
        return self._has_content(object_name)

    def _valid_object_content(self, object_content: bytes) -> bool:
        return self._has_content(object_content)

    def _has_content(self, element: Union[str, bytes]) -> bool:
        return element is not None and len(element) > 0

    def _delete_data(self, object_full_path: str):
        self.__s3_client.delete_object(Bucket=self.__s3_bucket, Key=object_full_path)

    def _list_all_schemas(self) -> SchemaMetadatas:
        items = self._list_files_from_path(SCHEMAS_LOCATION)
        if len(items) > 0:
            return SchemaMetadatas(
                [
                    SchemaMetadata.from_path(item["Key"], self)
                    for item in items
                    if item["Key"].endswith(".json")
                ]
            )
        return SchemaMetadatas.empty()
