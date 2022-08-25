import os
from typing import Union, Optional, List, Tuple, Dict

import boto3
import pandas as pd
from botocore.exceptions import ClientError
from botocore.response import StreamingBody

from api.common.config.auth import SensitivityLevel
from api.common.config.aws import DATA_BUCKET, SCHEMAS_LOCATION
from api.common.config.constants import CONTENT_ENCODING
from api.common.custom_exceptions import SchemaNotFoundError, UserError, AWSServiceError
from api.common.logger import AppLogger
from api.domain.schema import Schema
from api.domain.schema_metadata import SchemaMetadata, SchemaMetadatas
from api.domain.storage_metadata import StorageMetaData


class S3Adapter:
    def __init__(self, s3_client=boto3.client("s3"), s3_bucket=DATA_BUCKET):
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

    def find_schema(self, domain: str, dataset: str) -> Optional[Schema]:
        try:
            schema_metadata = self._retrieve_schema_metadata(
                domain=domain, dataset=dataset
            )
            dataset = self.retrieve_data(schema_metadata.schema_path())
            return Schema.parse_raw(dataset.read())
        except SchemaNotFoundError:
            return None
        except ClientError as error:
            if error.response["Error"]["Code"] == "NoSuchKey":
                return None

    def find_raw_file(self, domain: str, dataset: str, filename: str):
        try:
            self.retrieve_data(StorageMetaData(domain, dataset).raw_data_path(filename))
        except ClientError as error:
            if error.response["Error"]["Code"] == "NoSuchKey":
                raise UserError(f"The file [{filename}] does not exist")

    def save_schema(
        self, domain: str, dataset: str, sensitivity: str, schema: Schema
    ) -> str:
        schema_meta_data = SchemaMetadata(
            sensitivity=sensitivity, domain=domain, dataset=dataset
        )
        self.store_data(
            object_full_path=schema_meta_data.schema_path(),
            object_content=self._convert_to_bytes(schema.json(indent=True)),
        )
        return schema_meta_data.schema_name()

    def delete_schema(self, domain: str, dataset: str, sensitivity: str):
        schema_path = SchemaMetadata(
            sensitivity=sensitivity, domain=domain, dataset=dataset
        ).schema_path()
        self._delete_data(schema_path)

    def get_dataset_sensitivity(
        self, domain: Optional[str], dataset: Optional[str]
    ) -> SensitivityLevel:
        if not domain or not dataset:
            return SensitivityLevel.from_string("PUBLIC")
        schema_metadata = self._retrieve_schema_metadata(domain, dataset)
        return SensitivityLevel.from_string(schema_metadata.get_sensitivity())

    def upload_partitioned_data(
        self,
        domain: str,
        dataset: str,
        filename: str,
        partitioned_data: List[Tuple[str, pd.DataFrame]],
    ):

        for index, (partition_path, data) in enumerate(partitioned_data):
            AppLogger.info(
                f"Uploading partition {index + 1}/{len(partitioned_data)} for {domain}/{dataset}"
            )
            upload_path = self._construct_partitioned_data_path(
                partition_path, filename, domain, dataset
            )
            data_content = data.to_parquet(compression="gzip", index=False)
            self.store_data(upload_path, data_content)

    def upload_raw_data(
        self, domain: str, dataset: str, filename: str, file_contents: Union[bytes, str]
    ):
        raw_data_path = StorageMetaData(domain, dataset).raw_data_path(filename)
        self.store_data(raw_data_path, file_contents)

    def list_raw_files(self, domain: str, dataset: str):
        object_list = self._list_files_from_path(
            StorageMetaData(domain, dataset).raw_data_location()
        )
        return self._map_object_list_to_filename(object_list)

    def delete_dataset_files(self, domain: str, dataset: str, raw_data_filename: str):
        dataset_metadata = StorageMetaData(domain, dataset)
        files = self._list_files_from_path(dataset_metadata.location())

        files_to_delete = [
            {"Key": data_file["Key"]}
            for data_file in files
            if self._clean_filename(data_file["Key"])
            == self._clean_filename(raw_data_filename)
        ]

        files_to_delete.append(
            {"Key": dataset_metadata.raw_data_path(raw_data_filename)}
        )

        self._delete_objects(files_to_delete, raw_data_filename)

    def _clean_filename(self, file_key: str) -> str:
        return file_key.rsplit("/", 1)[-1].split(".")[0]

    def _construct_partitioned_data_path(
        self, partition_path: str, filename: str, domain: str, dataset: str
    ) -> str:
        dataset_meta_data = StorageMetaData(domain, dataset)
        return os.path.join(dataset_meta_data.location(), partition_path, filename)

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
                f"The file [{filename}] could not be deleted. Please contact your administrator."
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

    def _map_object_list_to_filename(self, object_list) -> list[str]:
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

    def _retrieve_schema_metadata(self, domain: str, dataset: str) -> SchemaMetadata:
        schemas = self._list_all_schemas()
        return schemas.find(domain=domain, dataset=dataset)

    def _list_all_schemas(self) -> SchemaMetadatas:
        items = self._list_files_from_path(SCHEMAS_LOCATION)
        if len(items) > 0:
            return SchemaMetadatas(
                [
                    SchemaMetadata.from_path(item["Key"])
                    for item in items
                    if item["Key"].endswith(".json")
                ]
            )
        return SchemaMetadatas.empty()
