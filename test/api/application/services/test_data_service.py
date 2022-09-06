import re
from pathlib import Path
from typing import List, Any
from unittest.mock import Mock, patch, MagicMock, call

import pandas as pd
import pytest
from botocore.exceptions import ClientError

from api.application.services.data_service import (
    DataService,
    construct_chunked_dataframe,
)
from api.common.config.constants import CONTENT_ENCODING
from api.common.custom_exceptions import (
    SchemaNotFoundError,
    CrawlerIsNotReadyError,
    SchemaValidationError,
    ConflictError,
    UserError,
    AWSServiceError,
    UnprocessableDatasetError,
    DatasetValidationError,
)
from api.domain.enriched_schema import (
    EnrichedSchema,
    EnrichedSchemaMetadata,
    EnrichedColumn,
)
from api.domain.schema import Schema, Column
from api.domain.schema_metadata import Owner, SchemaMetadata
from api.domain.sql_query import SQLQuery


class TestUploadSchema:
    def setup_method(self):
        self.s3_adapter = Mock()
        self.glue_adapter = Mock()
        self.query_adapter = Mock()
        self.protected_domain_service = Mock()
        self.cognito_adapter = Mock()
        self.data_service = DataService(
            self.s3_adapter,
            self.glue_adapter,
            self.query_adapter,
            self.protected_domain_service,
            self.cognito_adapter,
        )
        self.valid_schema = Schema(
            metadata=SchemaMetadata(
                domain="some",
                dataset="other",
                sensitivity="PUBLIC",
                owners=[Owner(name="owner", email="owner@email.com")],
            ),
            columns=[
                Column(
                    name="colname1",
                    partition_index=0,
                    data_type="Int64",
                    allow_null=False,
                ),
                Column(
                    name="colname2",
                    partition_index=None,
                    data_type="object",
                    allow_null=True,
                ),
            ],
        )

    def test_upload_schema(self):
        self.s3_adapter.find_schema.return_value = None
        self.s3_adapter.save_schema.return_value = "some-other.json"

        result = self.data_service.upload_schema(self.valid_schema)

        self.s3_adapter.save_schema.assert_called_once_with(self.valid_schema)
        self.glue_adapter.create_crawler.assert_called_once_with(
            "some", "other", {"sensitivity": "PUBLIC", "no_of_versions": "1"}
        )
        assert result == "some-other.json"

    def test_aborts_uploading_if_schema_upload_fails(self):
        self.s3_adapter.find_schema.return_value = None
        self.s3_adapter.save_schema.side_effect = ClientError(
            error_response={"Error": {"Code": "Failed"}}, operation_name="PutObject"
        )

        with pytest.raises(ClientError):
            self.data_service.upload_schema(self.valid_schema)

        self.cognito_adapter.create_user_groups.assert_not_called()
        self.glue_adapter.create_crawler.assert_not_called()

    def test_check_for_protected_domain_success(self):
        schema = Schema(
            metadata=SchemaMetadata(
                domain="domain",
                dataset="dataset",
                sensitivity="PROTECTED",
                owners=[Owner(name="owner", email="owner@email.com")],
            ),
            columns=[
                Column(
                    name="colname1",
                    partition_index=0,
                    data_type="Int64",
                    allow_null=True,
                ),
            ],
        )
        self.protected_domain_service.list_protected_domains = Mock(
            return_value=["domain", "other"]
        )

        result = self.data_service.check_for_protected_domain(schema)

        self.protected_domain_service.list_protected_domains.assert_called_once()
        assert result == "domain"

    def test_check_for_protected_domain_fails(self):
        schema = Schema(
            metadata=SchemaMetadata(
                domain="domain1",
                dataset="dataset2",
                sensitivity="PROTECTED",
                owners=[Owner(name="owner", email="owner@email.com")],
            ),
            columns=[
                Column(
                    name="colname1",
                    partition_index=0,
                    data_type="Int64",
                    allow_null=True,
                ),
            ],
        )
        self.protected_domain_service.list_protected_domains = Mock(
            return_value=["other"]
        )

        with pytest.raises(
            UserError, match="The protected domain 'domain1' does not exist."
        ):
            self.data_service.check_for_protected_domain(schema)

    def test_upload_schema_throws_error_when_schema_already_exists(self):
        self.s3_adapter.find_schema.return_value = self.valid_schema

        with pytest.raises(ConflictError, match="Schema already exists"):
            self.data_service.upload_schema(self.valid_schema)

    def test_upload_schema_throws_error_when_schema_invalid(self):
        self.s3_adapter.find_schema.return_value = None

        invalid_partition_index = -1
        invalid_schema = Schema(
            metadata=SchemaMetadata(
                domain="some",
                dataset="other",
                sensitivity="PUBLIC",
                owners=[Owner(name="owner", email="owner@email.com")],
            ),
            columns=[
                Column(
                    name="colname1",
                    partition_index=invalid_partition_index,
                    data_type="Int64",
                    allow_null=True,
                )
            ],
        )

        with pytest.raises(SchemaValidationError):
            self.data_service.upload_schema(invalid_schema)


class TestUploadDataset:
    def setup_method(self):
        self.s3_adapter = Mock()
        self.glue_adapter = Mock()
        self.query_adapter = Mock()
        self.protected_domain_service = Mock()
        self.data_service = DataService(
            self.s3_adapter,
            self.glue_adapter,
            self.query_adapter,
            self.protected_domain_service,
            None,
        )
        self.valid_schema = Schema(
            metadata=SchemaMetadata(
                domain="some",
                dataset="other",
                sensitivity="PUBLIC",
                owners=[Owner(name="owner", email="owner@email.com")],
            ),
            columns=[
                Column(
                    name="colname1",
                    partition_index=0,
                    data_type="Int64",
                    allow_null=True,
                ),
                Column(
                    name="colname2",
                    partition_index=None,
                    data_type="object",
                    allow_null=False,
                ),
            ],
        )

    def generator(self, data_list: List[Any]):
        for element in data_list:
            yield element

    def chunked_dataframe_values(
        self, mock_construct_chunked_dataframe, dataframes: List[pd.DataFrame]
    ):
        mock_test_file_reader = MagicMock()
        mock_construct_chunked_dataframe.return_value = mock_test_file_reader
        mock_test_file_reader.__iter__.return_value = dataframes
        return mock_test_file_reader

    # Dataset chunking -------------------------------
    @patch("api.application.services.data_service.pd")
    def test_construct_chunked_dataframe(self, mock_pd):
        path = Path("file/path")

        construct_chunked_dataframe(path)
        mock_pd.read_csv.assert_called_once_with(
            path, encoding=CONTENT_ENCODING, sep=",", chunksize=1_000_000
        )

    # Schema retrieval -------------------------------
    def test_raises_error_when_schema_does_not_exist(self):
        self.s3_adapter.find_schema.return_value = None

        with pytest.raises(SchemaNotFoundError):
            self.data_service.upload_dataset("some", "other", Path("data.csv"))

        self.s3_adapter.find_schema.assert_called_once_with("some", "other", 1)

    # Upload Dataset APPEND behaviour  -------------------------------------
    @patch("api.application.services.data_service.Thread")
    @patch("api.application.services.data_service.build_validated_dataframe")
    @patch("api.application.services.data_service.construct_chunked_dataframe")
    def test_upload_dataset_validates_each_chunk_of_the_dataset(
        self,
        mock_construct_chunked_dataframe,
        mock_build_validated_dataframe,
        _mock_thread,
    ):
        # Given
        schema = self.valid_schema
        self.s3_adapter.find_schema.return_value = schema

        chunk1 = pd.DataFrame({})
        chunk2 = pd.DataFrame({})
        chunk3 = pd.DataFrame({})

        mock_construct_chunked_dataframe.return_value = [
            chunk1,
            chunk2,
            chunk3,
        ]

        expected_calls = [
            call(schema, chunk1),
            call(schema, chunk2),
            call(schema, chunk3),
        ]

        # When
        self.data_service.upload_dataset("domain1", "dataset1", Path("data.csv"))

        mock_build_validated_dataframe.assert_has_calls(expected_calls)

    @patch("api.application.services.data_service.construct_chunked_dataframe")
    def test_upload_dataset_triggers_processing_manager(
        self, _mock_construct_chunked_dataframe
    ):
        # GIVEN
        schema = self.valid_schema

        self.s3_adapter.find_schema.return_value = schema

        self.data_service.generate_raw_file_identifier = Mock(
            return_value="123-456-789"
        )

        self.data_service.manage_processing = Mock()

        # WHEN
        uploaded_raw_file = self.data_service.upload_dataset(
            "some", "other", Path("data.csv")
        )

        # THEN
        self.glue_adapter.check_crawler_is_ready.assert_called_once_with(
            "some", "other"
        )

        self.data_service.generate_raw_file_identifier.assert_called_once()
        self.data_service.manage_processing.assert_called_once_with(
            schema, Path("data.csv"), "123-456-789"
        )
        assert uploaded_raw_file == "123-456-789.csv"

    @patch("api.application.services.data_service.Thread")
    @patch("api.application.services.data_service.os")
    def test_upload_processing_manager_starts_threads_and_deletes_raw_file(
        self, mock_os, mock_thread
    ):
        # GIVEN
        schema = self.valid_schema

        mock_thread_1 = Mock()
        mock_thread_2 = Mock()

        mock_thread_1.is_alive.return_value = False
        mock_thread_2.is_alive.return_value = False

        mock_thread.side_effect = [mock_thread_1, mock_thread_2]

        # WHEN
        self.data_service.manage_processing(schema, Path("data.csv"), "123-456-789")

        # THEN
        mock_thread_1.start.assert_called_once()
        mock_thread_2.start.assert_called_once()

        mock_thread_1.is_alive.assert_called_once()
        mock_thread_2.is_alive.assert_called_once()

        mock_os.remove.assert_called_once_with("data.csv")

    @patch("api.application.services.data_service.sleep")
    @patch("api.application.services.data_service.os")
    @patch.object(DataService, "process_chunks")
    def test_upload_processing_manager_calls_correct_processing_functions(
        self, mock_process_chunks, mock_os, _mock_sleep
    ):
        # GIVEN
        schema = self.valid_schema

        # WHEN
        self.data_service.manage_processing(schema, Path("data.csv"), "123-456-789")

        # THEN
        mock_process_chunks.assert_called_once_with(
            schema, Path("data.csv"), "123-456-789"
        )
        self.s3_adapter.upload_raw_data.assert_called_once_with(
            "some", "other", Path("data.csv"), "123-456-789"
        )
        mock_os.remove.assert_called_once_with("data.csv")

    # Process Chunks -----------------------------------------
    @patch("api.application.services.data_service.construct_chunked_dataframe")
    def test_processes_each_dataset_chunk_with_append_behaviour(
        self, mock_construct_chunked_dataframe
    ):
        # Given
        schema = self.valid_schema

        def dataset_chunk():
            return pd.DataFrame(
                {
                    "col1": ["one", "two", "three"],
                    "col2": [1, 2, 3],
                }
            )

        chunk1 = dataset_chunk()
        chunk2 = dataset_chunk()

        mock_construct_chunked_dataframe.return_value = [
            chunk1,
            chunk2,
        ]

        self.data_service.process_chunk = Mock()

        # When
        self.data_service.process_chunks(schema, Path("data.csv"), "123-456-789")

        # Then
        expected_calls = [
            call(schema, "123-456-789", chunk1),
            call(schema, "123-456-789", chunk2),
        ]
        self.data_service.process_chunk.assert_has_calls(expected_calls)
        self.s3_adapter.list_raw_files.assert_not_called()
        self.s3_adapter.delete_dataset_files.assert_not_called()

    @patch("api.application.services.data_service.construct_chunked_dataframe")
    def test_processes_each_dataset_chunk_with_overwrite_behaviour(
        self, mock_construct_chunked_dataframe
    ):
        # Given
        schema = Schema(
            metadata=SchemaMetadata(
                domain="some",
                dataset="other",
                sensitivity="PUBLIC",
                owners=[Owner(name="owner", email="owner@email.com")],
                update_behaviour="OVERWRITE",
            ),
            columns=[
                Column(
                    name="colname1",
                    partition_index=0,
                    data_type="Int64",
                    allow_null=True,
                ),
                Column(
                    name="colname2",
                    partition_index=None,
                    data_type="object",
                    allow_null=False,
                ),
            ],
        )

        def dataset_chunk():
            return pd.DataFrame(
                {
                    "col1": ["one", "two", "three"],
                    "col2": [1, 2, 3],
                }
            )

        chunk1 = dataset_chunk()
        chunk2 = dataset_chunk()

        mock_construct_chunked_dataframe.return_value = [
            chunk1,
            chunk2,
        ]

        self.data_service.process_chunk = Mock()

        self.s3_adapter.list_raw_files.return_value = [
            "123-456-789.csv",
            "987-654-321.csv",
        ]

        # When
        self.data_service.process_chunks(schema, Path("data.csv"), "123-456-789")

        # Then
        expected_calls = [
            call(schema, "123-456-789", chunk1),
            call(schema, "123-456-789", chunk2),
        ]
        self.data_service.process_chunk.assert_has_calls(expected_calls)
        self.s3_adapter.list_raw_files.assert_called_once_with("some", "other")
        self.s3_adapter.delete_dataset_files.assert_called_once_with(
            "some", "other", "987-654-321.csv"
        )

    @patch("api.application.services.data_service.construct_chunked_dataframe")
    def test_processes_each_dataset_chunk_with_overwrite_behaviour_has_no_files_to_override(
        self, mock_construct_chunked_dataframe
    ):
        # Given
        schema = Schema(
            metadata=SchemaMetadata(
                domain="some",
                dataset="other",
                sensitivity="PUBLIC",
                owners=[Owner(name="owner", email="owner@email.com")],
                update_behaviour="OVERWRITE",
            ),
            columns=[
                Column(
                    name="colname1",
                    partition_index=0,
                    data_type="Int64",
                    allow_null=True,
                ),
                Column(
                    name="colname2",
                    partition_index=None,
                    data_type="object",
                    allow_null=False,
                ),
            ],
        )

        mock_construct_chunked_dataframe.return_value = []

        self.data_service.process_chunk = Mock()

        self.s3_adapter.list_raw_files.return_value = ["123-456-789.csv"]

        # When
        self.data_service.process_chunks(schema, Path("data.csv"), "123-456-789")

        # Then
        self.s3_adapter.list_raw_files.assert_called_once_with("some", "other")
        self.s3_adapter.delete_dataset_files.assert_not_called()

    @patch("api.application.services.data_service.construct_chunked_dataframe")
    def test_processes_each_dataset_chunk_with_overwrite_behaviour_fails_to_delete_overidden_files(
        self, mock_construct_chunked_dataframe
    ):
        # Given
        schema = Schema(
            metadata=SchemaMetadata(
                domain="some",
                dataset="other",
                sensitivity="PUBLIC",
                owners=[Owner(name="owner", email="owner@email.com")],
                update_behaviour="OVERWRITE",
            ),
            columns=[
                Column(
                    name="colname1",
                    partition_index=0,
                    data_type="Int64",
                    allow_null=True,
                ),
                Column(
                    name="colname2",
                    partition_index=None,
                    data_type="object",
                    allow_null=False,
                ),
            ],
        )

        mock_construct_chunked_dataframe.return_value = []

        self.data_service.process_chunk = Mock()

        self.s3_adapter.list_raw_files.return_value = [
            "123-456-789.csv",
            "987-654-321.csv",
        ]

        self.s3_adapter.delete_dataset_files.side_effect = AWSServiceError("something")

        # When
        with pytest.raises(
            AWSServiceError,
            match=r"Overriding existing data failed for domain \[some\] and dataset \[other\]. Raw file identifier: 123-456-789",
        ):
            self.data_service.process_chunks(schema, Path("data.csv"), "123-456-789")

        # Then
        self.s3_adapter.list_raw_files.assert_called_once_with("some", "other")
        self.s3_adapter.delete_dataset_files.assert_called_once_with(
            "some", "other", "987-654-321.csv"
        )

    @patch("api.application.services.data_service.construct_chunked_dataframe")
    def test_starts_crawler_and_updates_catalog_table_config(
        self, mock_construct_chunked_dataframe
    ):
        # Given
        schema = self.valid_schema

        mock_construct_chunked_dataframe.return_value = []

        # When
        self.data_service.process_chunks(schema, Path("data.csv"), "123-456-789")

        # Then
        self.glue_adapter.start_crawler.assert_called_once_with("some", "other")
        self.glue_adapter.update_catalog_table_config.assert_called_once_with(schema)

    # Process Chunks -----------------------------------------
    @patch("api.application.services.data_service.build_validated_dataframe")
    def test_validates_and_uploads_chunk(self, mock_build_validated_dataframe):
        # Given
        schema = self.valid_schema

        chunk = pd.DataFrame({})
        chunk2 = pd.DataFrame({})

        mock_build_validated_dataframe.return_value = chunk2
        self.data_service.upload_data = Mock()
        self.data_service.generate_permanent_filename = Mock(
            return_value="123-456-789_111-222-333.parquet"
        )

        # When
        self.data_service.process_chunk(schema, "123-456-789", chunk)

        # Then
        self.data_service.upload_data.assert_called_once_with(
            schema, chunk2, "123-456-789_111-222-333.parquet"
        )

    @patch("api.application.services.data_service.build_validated_dataframe")
    def test_raises_validation_error_when_validation_fails(
        self, mock_build_validated_dataframe
    ):
        # Given
        schema = self.valid_schema

        chunk = pd.DataFrame({})

        mock_build_validated_dataframe.side_effect = DatasetValidationError(
            "some error"
        )

        # When/Then
        with pytest.raises(DatasetValidationError, match="some error"):
            self.data_service.process_chunk(schema, "123-456-789", chunk)

    # Upload Data --------------------------------------------
    @patch("api.application.services.data_service.generate_partitioned_data")
    def test_partitions_and_uploads_data(self, mock_generate_partitioned_data):
        # Given
        schema = self.valid_schema

        dataframe = pd.DataFrame({})
        filename = "11111111_22222222.parquet"

        partitioned_dataframe = [
            ("some/path1", pd.DataFrame({})),
            ("some/path2", pd.DataFrame({})),
        ]
        mock_generate_partitioned_data.return_value = partitioned_dataframe

        # When
        self.data_service.upload_data(schema, dataframe, filename)

        # Then
        self.s3_adapter.upload_partitioned_data.assert_called_once_with(
            "some", "other", filename, partitioned_dataframe
        )

    # Generate Permanent Filename ----------------------------
    @patch("api.application.services.data_service.uuid")
    def test_generates_permanent_filename(self, mock_uuid):
        # Given
        raw_file_identifier = "123-456-789"

        mock_uuid.uuid4.return_value = "111-222-333"

        # When
        result = self.data_service.generate_permanent_filename(raw_file_identifier)

        # Then
        assert result == "123-456-789_111-222-333.parquet"

    # Dataset chunk validation -------------------------------
    @patch("api.application.services.data_service.construct_chunked_dataframe")
    def test_upload_dataset_in_chunks_with_invalid_column_headers(
        self, mock_construct_chunked_dataframe
    ):
        schema = self.valid_schema

        self.s3_adapter.find_schema.return_value = schema

        self.chunked_dataframe_values(
            mock_construct_chunked_dataframe,
            [
                pd.DataFrame(
                    {"colname1": [1234, 4567], "colnamewrong": ["Carlos", "Ada"]}
                )
            ],
        )

        with pytest.raises(UnprocessableDatasetError):
            self.data_service.upload_dataset("some", "other", Path("data.csv"))

    @patch("api.application.services.data_service.construct_chunked_dataframe")
    def test_upload_dataset_in_chunks_with_invalid_data(
        self, mock_construct_chunked_dataframe
    ):
        schema = self.valid_schema

        self.s3_adapter.find_schema.return_value = schema

        self.chunked_dataframe_values(
            mock_construct_chunked_dataframe,
            [
                pd.DataFrame({"colname1": [1234, 4567], "colname2": ["Carlos", "Ada"]}),
                pd.DataFrame(
                    {"colname1": [4332, "s2134"], "colname2": ["Carlos", "Ada"]}
                ),
                pd.DataFrame(
                    {"colname1": [3543, 456743], "colname2": ["Carlos", "Ada"]}
                ),
            ],
        )

        try:
            self.data_service.upload_dataset("some", "other", Path("data.csv"))
        except DatasetValidationError as error:
            assert {
                "Failed to convert column [colname1] to type [Int64]",
                "Column [colname1] has an incorrect data type. Expected Int64, received "
                "object",
            }.issubset(error.message)

    @patch("api.application.services.data_service.construct_chunked_dataframe")
    def test_upload_dataset_in_chunks_with_invalid_data_in_multiple_chunks(
        self, mock_construct_chunked_dataframe
    ):
        schema = self.valid_schema

        self.s3_adapter.find_schema.return_value = schema

        self.chunked_dataframe_values(
            mock_construct_chunked_dataframe,
            [
                pd.DataFrame({"colname1": [1234, 4567], "colname2": ["Carlos", "Ada"]}),
                pd.DataFrame(
                    {"colname1": [4332, "s2134"], "colname2": ["Carlos", "Ada"]}
                ),
                pd.DataFrame(
                    {"colname1": [4332, "s2134"], "colname2": ["Carlos", "Ada"]}
                ),
                pd.DataFrame(
                    {"colname1": [3543, 456743], "colname2": ["Carlos", "Ada"]}
                ),
            ],
        )

        try:
            self.data_service.upload_dataset("some", "other", Path("data.csv"))
        except DatasetValidationError as error:
            assert {
                "Column [colname1] has an incorrect data type. Expected Int64, received "
                "object",
                "Failed to convert column [colname1] to type [Int64]",
            }.issubset(error.message)

    # Crawler state and trigger errors ----------------------
    def test_upload_dataset_fails_when_unable_to_get_crawler_state(self):
        self.s3_adapter.find_schema.return_value = self.valid_schema

        self.glue_adapter.check_crawler_is_ready.side_effect = AWSServiceError(
            "Some message"
        )

        with pytest.raises(AWSServiceError, match="Some message"):
            self.data_service.upload_dataset("some", "other", Path("data.csv"))

        self.glue_adapter.check_crawler_is_ready.assert_called_once_with(
            "some", "other"
        )

    def test_upload_dataset_fails_when_crawler_is_not_ready(self):
        self.s3_adapter.find_schema.return_value = self.valid_schema

        self.glue_adapter.check_crawler_is_ready.side_effect = CrawlerIsNotReadyError(
            "msg"
        )

        with pytest.raises(CrawlerIsNotReadyError):
            self.data_service.upload_dataset("some", "other", Path("data.csv"))

        self.glue_adapter.check_crawler_is_ready.assert_called_once_with(
            "some", "other"
        )


class TestListRawFiles:
    def setup_method(self):
        self.s3_adapter = Mock()
        self.data_service = DataService(
            self.s3_adapter,
            None,
            None,
            None,
            None,
        )

    def test_list_raw_files_from_domain_and_dataset(self):
        self.s3_adapter.list_raw_files.return_value = [
            "2022-01-01T12:00:00-my_first_file.csv",
            "2022-02-10T15:00:00-my_second_file.csv",
            "2022-03-03T16:00:00-my_third_file.csv",
        ]
        list_raw_files = self.data_service.list_raw_files("domain", "dataset")
        assert list_raw_files == [
            "2022-01-01T12:00:00-my_first_file.csv",
            "2022-02-10T15:00:00-my_second_file.csv",
            "2022-03-03T16:00:00-my_third_file.csv",
        ]
        self.s3_adapter.list_raw_files.assert_called_once_with("domain", "dataset")

    def test_raises_exception_when_no_raw_files_found_for_domain_and_dataset(self):
        self.s3_adapter.list_raw_files.return_value = []
        with pytest.raises(
            UserError,
            match="There are no uploaded files for the domain \\[domain\\] or dataset \\[dataset\\]",
        ):
            self.data_service.list_raw_files("domain", "dataset")

        self.s3_adapter.list_raw_files.assert_called_once_with("domain", "dataset")


class TestDatasetInfoRetrieval:
    def setup_method(self):
        self.s3_adapter = Mock()
        self.glue_adapter = Mock()
        self.query_adapter = Mock()
        self.aws_resource_adapter = Mock()
        self.protected_domain_service = Mock()
        self.valid_schema = Schema(
            metadata=SchemaMetadata(
                domain="some",
                dataset="other",
                sensitivity="PUBLIC",
                version=2,
                owners=[Owner(name="owner", email="owner@email.com")],
            ),
            columns=[
                Column(
                    name="colname1",
                    partition_index=0,
                    data_type="Int64",
                    allow_null=False,
                ),
                Column(
                    name="colname2",
                    partition_index=None,
                    data_type="object",
                    allow_null=False,
                ),
                Column(
                    name="date",
                    partition_index=None,
                    data_type="date",
                    allow_null=False,
                    format="%d/%m/%Y",
                ),
            ],
        )
        self.data_service = DataService(
            self.s3_adapter,
            self.glue_adapter,
            self.query_adapter,
            self.protected_domain_service,
            None,
        )
        self.glue_adapter.get_table_last_updated_date.return_value = (
            "2022-03-01 11:03:49+00:00"
        )

    def test_get_schema_information(self):
        expected_schema = EnrichedSchema(
            metadata=EnrichedSchemaMetadata(
                domain="some",
                dataset="other",
                sensitivity="PUBLIC",
                version=2,
                owners=[Owner(name="owner", email="owner@email.com")],
                number_of_rows=48718,
                number_of_columns=3,
                last_updated="2022-03-01 11:03:49+00:00",
            ),
            columns=[
                EnrichedColumn(
                    name="colname1",
                    partition_index=0,
                    data_type="Int64",
                    allow_null=False,
                ),
                EnrichedColumn(
                    name="colname2",
                    partition_index=None,
                    data_type="object",
                    allow_null=False,
                ),
                EnrichedColumn(
                    name="date",
                    partition_index=None,
                    data_type="date",
                    allow_null=False,
                    format="%d/%m/%Y",
                    statistics={"max": "2021-07-01", "min": "2014-01-01"},
                ),
            ],
        )
        self.s3_adapter.find_schema.return_value = self.valid_schema
        self.query_adapter.query.return_value = pd.DataFrame(
            {
                "data_size": [48718],
                "max_date": ["2021-07-01"],
                "min_date": ["2014-01-01"],
            }
        )
        actual_schema = self.data_service.get_dataset_info("some", "other", 2)

        self.query_adapter.query.assert_called_once_with(
            "some",
            "other",
            2,
            SQLQuery(
                select_columns=[
                    "count(*) as data_size",
                    "max(date) as max_date",
                    "min(date) as min_date",
                ]
            ),
        )
        assert actual_schema == expected_schema

    @patch("api.application.services.data_service.handle_version_retrieval")
    def test_get_schema_information_for_multiple_dates(
        self, mock_handle_version_retrieval
    ):
        valid_schema = Schema(
            metadata=SchemaMetadata(
                domain="some",
                dataset="other",
                sensitivity="PUBLIC",
                version=1,
                owners=[Owner(name="owner", email="owner@email.com")],
            ),
            columns=[
                Column(
                    name="colname1",
                    partition_index=0,
                    data_type="Int64",
                    allow_null=False,
                ),
                Column(
                    name="date",
                    partition_index=None,
                    data_type="date",
                    allow_null=False,
                    format="%d/%m/%Y",
                ),
                Column(
                    name="date2",
                    partition_index=None,
                    data_type="date",
                    allow_null=False,
                    format="%d/%m/%Y",
                ),
            ],
        )

        expected_schema = EnrichedSchema(
            metadata=EnrichedSchemaMetadata(
                domain="some",
                dataset="other",
                sensitivity="PUBLIC",
                version=1,
                owners=[Owner(name="owner", email="owner@email.com")],
                number_of_rows=48718,
                number_of_columns=3,
                last_updated="2022-03-01 11:03:49+00:00",
            ),
            columns=[
                EnrichedColumn(
                    name="colname1",
                    partition_index=0,
                    data_type="Int64",
                    allow_null=False,
                ),
                EnrichedColumn(
                    name="date",
                    partition_index=None,
                    data_type="date",
                    allow_null=False,
                    format="%d/%m/%Y",
                    statistics={"max": "2021-07-01", "min": "2014-01-01"},
                ),
                EnrichedColumn(
                    name="date2",
                    partition_index=None,
                    data_type="date",
                    allow_null=False,
                    format="%d/%m/%Y",
                    statistics={"max": "2020-07-01", "min": "2015-01-01"},
                ),
            ],
        )
        self.s3_adapter.find_schema.return_value = valid_schema
        self.query_adapter.query.return_value = pd.DataFrame(
            {
                "data_size": [48718],
                "max_date": ["2021-07-01"],
                "min_date": ["2014-01-01"],
                "max_date2": ["2020-07-01"],
                "min_date2": ["2015-01-01"],
            }
        )

        mock_handle_version_retrieval.return_value = 1
        actual_schema = self.data_service.get_dataset_info("some", "other", None)

        self.query_adapter.query.assert_called_once_with(
            "some",
            "other",
            1,
            SQLQuery(
                select_columns=[
                    "count(*) as data_size",
                    "max(date) as max_date",
                    "max(date2) as max_date2",
                    "min(date) as min_date",
                    "min(date2) as min_date2",
                ]
            ),
        )
        mock_handle_version_retrieval.assert_called_once_with("some", "other", None)

        assert actual_schema == expected_schema

    def test_get_schema_size_for_datasets_with_no_dates(self):
        valid_schema = Schema(
            metadata=SchemaMetadata(
                domain="some",
                dataset="other",
                sensitivity="PUBLIC",
                version=3,
                owners=[Owner(name="owner", email="owner@email.com")],
            ),
            columns=[
                Column(
                    name="colname1",
                    partition_index=0,
                    data_type="Int64",
                    allow_null=False,
                )
            ],
        )
        expected_schema = EnrichedSchema(
            metadata=EnrichedSchemaMetadata(
                domain="some",
                dataset="other",
                sensitivity="PUBLIC",
                version=3,
                owners=[Owner(name="owner", email="owner@email.com")],
                number_of_rows=48718,
                number_of_columns=1,
                last_updated="2022-03-01 11:03:49+00:00",
            ),
            columns=[
                EnrichedColumn(
                    name="colname1",
                    partition_index=0,
                    data_type="Int64",
                    allow_null=False,
                )
            ],
        )
        self.s3_adapter.find_schema.return_value = valid_schema
        self.query_adapter.query.return_value = pd.DataFrame({"data_size": [48718]})

        actual_schema = self.data_service.get_dataset_info("some", "other", 3)

        self.query_adapter.query.assert_called_once_with(
            "some", "other", 3, SQLQuery(select_columns=["count(*) as data_size"])
        )

        assert actual_schema == expected_schema

    def test_raises_error_when_schema_not_found(self):
        self.s3_adapter.find_schema.return_value = None

        with pytest.raises(SchemaNotFoundError):
            self.data_service.get_dataset_info("some", "other", 1)

    def test_generates_raw_file_identifier(self):
        filename = self.data_service.generate_raw_file_identifier()
        pattern = "[\\d\\w]{8}-[\\d\\w]{4}-[\\d\\w]{4}-[\\d\\w]{4}-[\\d\\w]{12}"
        assert re.match(pattern, filename)
