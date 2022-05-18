from unittest.mock import Mock, call

import pandas as pd
import pytest
from botocore.exceptions import ClientError

from api.adapter.s3_adapter import S3Adapter
from api.common.config.auth import SensitivityLevel
from api.common.config.aws import SCHEMAS_LOCATION
from api.common.custom_exceptions import (
    UserError,
    AWSServiceError,
)
from api.domain.schema import Schema, SchemaMetadata, Owner, Column
from test.test_utils import (
    set_encoded_content,
    mock_schema_response,
    mock_list_schemas_response,
)


class TestS3AdapterUpload:
    mock_s3_client = None
    persistence_adapter = None

    def setup_method(self):
        self.mock_s3_client = Mock()
        self.persistence_adapter = S3Adapter(
            s3_client=self.mock_s3_client, s3_bucket="dataset"
        )

    def _assert_store_data_raises(self, exception, message, filename, object_content):
        with pytest.raises(exception, match=message):
            self.persistence_adapter.store_data(
                object_full_path=filename, object_content=object_content
            )

    def test_data_upload(self):
        filename = "test_journey_file.csv"
        file_content = b"colname1,colname2\nsomething,123\notherthing,456\n\n"

        self.persistence_adapter.store_data(
            object_full_path=filename, object_content=file_content
        )

        self.mock_s3_client.put_object.assert_called_with(
            Bucket="dataset",
            Key=filename,
            Body=file_content,
        )

    def test_store_data_throws_exception_when_file_name_is_not_provided(self):
        self._assert_store_data_raises(
            exception=UserError,
            message="File path is invalid",
            filename=None,
            object_content="something",
        )

    def test_store_data_throws_exception_when_file_name_is_empty(self):
        self._assert_store_data_raises(
            exception=UserError,
            message="File path is invalid",
            filename="",
            object_content="something",
        )

    def test_store_data_throws_exception_when_file_contents_empty(self):
        self._assert_store_data_raises(
            exception=UserError,
            message="File content is invalid",
            filename="filename.csv",
            object_content="",
        )

    def test_upload_partitioned_data(self):
        domain = "domain"
        dataset = "dataset"
        filename = "data.csv"
        partitioned_data = [
            ("year=2020/month=1", pd.DataFrame({"colname2": ["user1"]})),
            ("year=2020/month=2", pd.DataFrame({"colname2": ["user2"]})),
        ]

        self.persistence_adapter.upload_partitioned_data(
            domain, dataset, filename, partitioned_data
        )

        calls = [
            call(
                Bucket="dataset",
                Key="data/domain/dataset/year=2020/month=1/data.csv",
                Body=set_encoded_content("colname2\n" "user1\n"),
            ),
            call(
                Bucket="dataset",
                Key="data/domain/dataset/year=2020/month=2/data.csv",
                Body=set_encoded_content("colname2\n" "user2\n"),
            ),
        ]

        self.mock_s3_client.put_object.assert_has_calls(calls)

    def test_schema_upload(self):
        valid_schema = Schema(
            metadata=SchemaMetadata(
                domain="test_domain",
                dataset="test_dataset",
                sensitivity="PUBLIC",
                owners=[Owner(name="owner", email="owner@email.com")],
            ),
            columns=[
                Column(
                    name="colname1",
                    partition_index=0,
                    data_type="Int64",
                    allow_null=True,
                )
            ],
        )

        result = self.persistence_adapter.save_schema(
            domain="test_domain",
            dataset="test_dataset",
            sensitivity="PUBLIC",
            schema=valid_schema,
        )

        self.mock_s3_client.put_object.assert_called_with(
            Bucket="dataset",
            Key="data/schemas/PUBLIC/test_domain-test_dataset.json",
            Body=b'{\n "metadata": {\n  "domain": "test_domain",\n  "dataset": "test_dataset",\n  "sensitivity": "PUBLIC",\n  "key_value_tags": {},\n  "key_only_tags": [],\n  "owners": [\n   {\n    "name": "owner",\n    "email": "owner@email.com"\n   }\n  ]\n },\n "columns": [\n  {\n   "name": "colname1",\n   "partition_index": 0,\n   "data_type": "Int64",\n   "allow_null": true,\n   "format": null\n  }\n ]\n}',
        )

        assert result == "test_domain-test_dataset.json"

    def test_raw_data_upload(self):
        file_contents = b"value,data\n1,2\n1,12"

        self.persistence_adapter.upload_raw_data(
            domain="some",
            dataset="values",
            filename="filename.csv",
            file_contents=file_contents,
        )

        self.mock_s3_client.put_object.assert_called_with(
            Bucket="dataset",
            Key="raw_data/some/values/filename.csv",
            Body=file_contents,
        )


class TestS3AdapterDataRetrieval:
    mock_s3_client = None
    persistence_adapter = None

    def setup_method(self):
        self.mock_s3_client = Mock()
        self.persistence_adapter = S3Adapter(
            s3_client=self.mock_s3_client, s3_bucket="dataset"
        )

    def test_retrieve_data(self):
        self.persistence_adapter.retrieve_data(key="an_s3_object")
        self.mock_s3_client.get_object.assert_called_once()

    def test_retrieve_existing_schema(self):
        domain = "test_domain"
        dataset = "test_dataset"

        valid_schema = Schema(
            metadata=SchemaMetadata(
                domain=domain, dataset=dataset, sensitivity="PUBLIC"
            ),
            columns=[
                Column(
                    name="colname1",
                    partition_index=0,
                    data_type="Int64",
                    allow_null=True,
                )
            ],
        )

        self.mock_s3_client.get_object.return_value = mock_schema_response()
        self.mock_s3_client.list_objects.return_value = mock_list_schemas_response()
        schema = self.persistence_adapter.find_schema(domain=domain, dataset=dataset)
        self.mock_s3_client.list_objects.assert_called_once_with(
            Bucket="dataset", Prefix="data/schemas"
        )
        self.mock_s3_client.get_object.assert_called_once_with(
            Bucket="dataset", Key="data/schemas/PUBLIC/test_domain-test_dataset.json"
        )

        assert schema == valid_schema

    def test_retrieve_non_existent_schema(self):
        self.mock_s3_client.list_objects.return_value = mock_list_schemas_response()
        schema = self.persistence_adapter.find_schema("bad", "data")

        self.mock_s3_client.list_objects.assert_called_once_with(
            Bucket="dataset", Prefix="data/schemas"
        )

        assert schema is None

    def test_find_raw_file_when_file_exists(self):
        self.persistence_adapter.find_raw_file("domain", "dataset", "filename.csv")
        self.mock_s3_client.get_object.assert_called_once_with(
            Bucket="dataset", Key="raw_data/domain/dataset/filename.csv"
        )

    def test_throws_error_for_find_raw_file_when_file__does_not_exist(self):
        self.mock_s3_client.get_object.side_effect = ClientError(
            error_response={
                "Error": {"Code": "NoSuchKey"},
            },
            operation_name="message",
        )

        with pytest.raises(UserError, match="The file \\[bad_file\\] does not exist"):
            self.persistence_adapter.find_raw_file("domain", "dataset", "bad_file")

        self.mock_s3_client.get_object.assert_called_once_with(
            Bucket="dataset", Key="raw_data/domain/dataset/bad_file"
        )


class TestS3Deletion:
    mock_s3_client = None
    persistence_adapter = None

    def setup_method(self):
        self.mock_s3_client = Mock()
        self.persistence_adapter = S3Adapter(
            s3_client=self.mock_s3_client, s3_bucket="data-bucket"
        )

    def test_deletion_of_schema(self):
        self.persistence_adapter.delete_schema("domain", "dataset", "PUBLIC")

        self.mock_s3_client.delete_object.assert_called_once_with(
            Bucket="data-bucket", Key="data/schemas/PUBLIC/domain-dataset.json"
        )

    def test_deletion_of_raw_files_with_no_partitions(self):
        self.mock_s3_client.list_objects.return_value = {
            "Contents": [
                {"Key": "data/domain/dataset/2022-03-10T12:00:00-test_file.csv"},
                {"Key": "data/domain/dataset/2020-01-01T12:00:00-file1.csv"},
                {"Key": "data/domain/dataset/2020-01-01T12:00:00-file2.csv"},
            ],
            "Name": "data-bucket",
            "Prefix": "data/domain/dataset",
            "EncodingType": "url",
        }
        self.mock_s3_client.delete_objects.return_value = {
            "Deleted": [
                {
                    "Key": "data/domain/dataset/2022-03-10T12:00:00-test_file.csv",
                },
                {
                    "Key": "raw_data/domain/dataset/2022-03-10T12:00:00-test_file.csv",
                },
            ],
        }

        self.persistence_adapter.delete_dataset_files(
            "domain", "dataset", "2022-03-10T12:00:00-test_file.csv"
        )
        self.mock_s3_client.list_objects.assert_called_once_with(
            Bucket="data-bucket", Prefix="data/domain/dataset"
        )

        self.mock_s3_client.delete_objects.assert_called_once_with(
            Bucket="data-bucket",
            Delete={
                "Objects": [
                    {
                        "Key": "data/domain/dataset/2022-03-10T12:00:00-test_file.csv",
                    },
                    {
                        "Key": "raw_data/domain/dataset/2022-03-10T12:00:00-test_file.csv",
                    },
                ],
            },
        )

    def test_deletion_of_raw_files_with_partitions(self):
        self.mock_s3_client.list_objects.return_value = {
            "Contents": [
                {
                    "Key": "data/domain/dataset/2022/03/2022-03-10T12:00:00-test_file.csv"
                },
                {"Key": "data/domain/dataset/2022/03/2020-05-01T12:00:00-file1.csv"},
                {"Key": "data/domain/dataset/2022/02/2020-01-01T12:00:00-file2.csv"},
                {
                    "Key": "data/domain/dataset/2022/02/2022-03-10T12:00:00-test_file.csv"
                },
            ],
            "Name": "data-bucket",
            "Prefix": "data/domain/dataset",
            "EncodingType": "url",
        }
        self.mock_s3_client.delete_objects.return_value = {
            "Deleted": [
                {
                    "Key": "data/domain/dataset/2022/03/2022-03-10T12:00:00-test_file.csv",
                },
                {
                    "Key": "data/domain/dataset/2022/02/2022-03-10T12:00:00-test_file.csv",
                },
                {
                    "Key": "raw_data/domain/dataset/2022-03-10T12:00:00-test_file.csv",
                },
            ]
        }

        self.persistence_adapter.delete_dataset_files(
            "domain", "dataset", "2022-03-10T12:00:00-test_file.csv"
        )
        self.mock_s3_client.list_objects.assert_called_once_with(
            Bucket="data-bucket", Prefix="data/domain/dataset"
        )

        self.mock_s3_client.delete_objects.assert_called_once_with(
            Bucket="data-bucket",
            Delete={
                "Objects": [
                    {
                        "Key": "data/domain/dataset/2022/03/2022-03-10T12:00:00-test_file.csv",
                    },
                    {
                        "Key": "data/domain/dataset/2022/02/2022-03-10T12:00:00-test_file.csv",
                    },
                    {
                        "Key": "raw_data/domain/dataset/2022-03-10T12:00:00-test_file.csv",
                    },
                ],
            },
        )

    def test_deletion_of_raw_files_when_error_is_thrown(self):
        self.mock_s3_client.list_objects.return_value = {}

        self.mock_s3_client.delete_objects.return_value = {
            "Errors": [
                {
                    "Key": "An error",
                    "VersionId": "has occurred",
                    "Code": "403 Forbidden",
                    "Message": "There is a problem with your Amazon Web Services account",
                },
                {
                    "Key": "Another error",
                    "VersionId": "has occurred",
                    "Code": "403 Forbidden",
                    "Message": "There is a problem with your Amazon Web Services account",
                },
            ]
        }
        msg = "The file \\[2022-03-10T12:00:00-test_file.csv\\] could not be deleted. Please contact your administrator."

        with pytest.raises(AWSServiceError, match=msg):
            self.persistence_adapter.delete_dataset_files(
                "domain", "dataset", "2022-03-10T12:00:00-test_file.csv"
            )

        self.mock_s3_client.list_objects.assert_called_once_with(
            Bucket="data-bucket", Prefix="data/domain/dataset"
        )


class TestDatasetMetadataRetrieval:
    mock_s3_client = None
    persistence_adapter = None

    def setup_method(self):
        self.mock_s3_client = Mock()
        self.persistence_adapter = S3Adapter(
            s3_client=self.mock_s3_client, s3_bucket="data-bucket"
        )

    @pytest.mark.parametrize(
        "domain, dataset, sensitivity, expected",
        [
            ("test_domain", "test_dataset", "PUBLIC", SensitivityLevel.PUBLIC),
            ("sample", "other", "PRIVATE", SensitivityLevel.PRIVATE),
            ("hi", "there", "PROTECTED", SensitivityLevel.PROTECTED),
        ],
    )
    def test_retrieves_dataset_sensitivity(
        self, domain: str, dataset: str, sensitivity: str, expected: SensitivityLevel
    ):
        self.mock_s3_client.list_objects.return_value = mock_list_schemas_response(
            domain, dataset, sensitivity
        )

        result = self.persistence_adapter.get_dataset_sensitivity(domain, dataset)

        self.mock_s3_client.list_objects.assert_called_once_with(
            Bucket="data-bucket", Prefix=SCHEMAS_LOCATION
        )

        assert result == expected

    def test_returns_none_if_not_schemas_exist(self):
        domain, dataset = "test_domain", "test_dataset"
        self.mock_s3_client.list_objects.return_value = {}

        result = self.persistence_adapter.find_schema(domain, dataset)

        self.mock_s3_client.list_objects.assert_called_once_with(
            Bucket="data-bucket", Prefix=SCHEMAS_LOCATION
        )

        assert result is None

    @pytest.mark.parametrize(
        "domain, dataset",
        [
            (None, None),
            ("domain", None),
            (None, "dataset"),
        ],
    )
    def test_returns_none_when_either_domain_or_dataset_or_both_not_specified(
        self, domain: str, dataset: str
    ):
        result = self.persistence_adapter.get_dataset_sensitivity(domain, dataset)

        assert result is SensitivityLevel.PUBLIC


class TestS3FileList:
    mock_s3_client = None
    persistence_adapter = None

    def setup_method(self):
        self.mock_s3_client = Mock()
        self.persistence_adapter = S3Adapter(
            s3_client=self.mock_s3_client, s3_bucket="my-bucket"
        )

    def test_list_raw_files(self):
        self.mock_s3_client.list_objects.return_value = {
            "Contents": [
                {"Key": "raw_data/my_domain/my_dataset/"},
                {"Key": "raw_data/my_domain/my_dataset/2020-01-01T12:00:00-file1.csv"},
                {"Key": "raw_data/my_domain/my_dataset/2020-06-01T15:00:00-file2.csv"},
                {
                    "Key": "raw_data/my_domain/my_dataset/2020-11-15T16:00:00-file3.csv",
                },
            ],
            "Name": "my-bucket",
            "Prefix": "raw_data/my_domain/my_dataset",
            "EncodingType": "url",
        }

        raw_files = self.persistence_adapter.list_raw_files("my_domain", "my_dataset")
        assert raw_files == [
            "2020-01-01T12:00:00-file1.csv",
            "2020-06-01T15:00:00-file2.csv",
            "2020-11-15T16:00:00-file3.csv",
        ]

        self.mock_s3_client.list_objects.assert_called_once_with(
            Bucket="my-bucket", Prefix="raw_data/my_domain/my_dataset"
        )

    def test_list_raw_files_when_empty(self):
        self.mock_s3_client.list_objects.return_value = {
            "Name": "my-bucket",
            "Prefix": "raw_data/my_domain/my_dataset",
            "EncodingType": "url",
        }

        raw_files = self.persistence_adapter.list_raw_files("my_domain", "my_dataset")
        assert raw_files == []

        self.mock_s3_client.list_objects.assert_called_once_with(
            Bucket="my-bucket", Prefix="raw_data/my_domain/my_dataset"
        )

    def test_list_raw_files_when_empty_response(self):
        self.mock_s3_client.list_objects.return_value = {}

        raw_files = self.persistence_adapter.list_raw_files("my_domain", "my_dataset")
        assert raw_files == []

        self.mock_s3_client.list_objects.assert_called_once_with(
            Bucket="my-bucket", Prefix="raw_data/my_domain/my_dataset"
        )
