import re
from pathlib import Path
from typing import List
from unittest.mock import Mock, patch, MagicMock

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
    CrawlerStartFailsError,
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
from api.domain.schema_metadata import Owner, UpdateBehaviour, SchemaMetadata
from api.domain.sql_query import SQLQuery
from test.test_utils import set_encoded_content


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

    def chunked_dataframe_values(
        self, mock_construct_chunked_dataframe, dataframes: List[pd.DataFrame]
    ):
        mock_test_file_reader = MagicMock()
        mock_construct_chunked_dataframe.return_value = mock_test_file_reader
        mock_test_file_reader.__iter__.return_value = dataframes
        return mock_test_file_reader

    # Happy Path -------------------------------------
    @patch("api.application.services.data_service.construct_chunked_dataframe")
    @patch("api.application.services.data_service.generate_partitioned_data")
    def test_upload_dataset_in_one_not_partitioned_file(
        self, mock_partitioner, mock_construct_chunked_dataframe
    ):
        schema = Schema(
            metadata=SchemaMetadata(
                domain="some",
                dataset="other",
                sensitivity="PUBLIC",
                owners=[Owner(name="owner", email="owner@email.com")],
            ),
            columns=[
                Column(
                    name="colname1",
                    partition_index=None,
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

        self.s3_adapter.find_schema.return_value = schema

        expected_df = pd.DataFrame(
            {
                "colname1": [1234, 4567],
                "colname2": ["Carlos", "Ada"],
            }
        )

        expected_df["colname1"] = expected_df["colname1"].astype(dtype=pd.Int64Dtype())

        self.chunked_dataframe_values(mock_construct_chunked_dataframe, [expected_df])

        partitioned_data = [("", expected_df)]
        mock_partitioner.return_value = partitioned_data

        self.data_service.generate_raw_filename = Mock(
            return_value="2022-03-03T12:00:00-data.csv"
        )

        permanent_filename = self.data_service.upload_dataset(
            "some", "other", Path("data.csv"), "data.csv"
        )
        assert permanent_filename == "2022-03-03T12:00:00-data.parquet"

        self.s3_adapter.upload_partitioned_data.assert_called_once_with(
            "some", "other", "2022-03-03T12:00:00-data.parquet", partitioned_data
        )

        self.glue_adapter.update_catalog_table_config.assert_called_once_with(schema)

        self.data_service.generate_raw_filename.assert_called_once_with("data.csv")

    @patch("api.application.services.data_service.construct_chunked_dataframe")
    @patch("api.application.services.data_service.generate_partitioned_data")
    def test_upload_dataset_in_two_partitioned_files(
        self, mock_partitioner, mock_construct_chunked_dataframe
    ):
        expected = pd.DataFrame(
            {
                "colname1": [1234, 4567],
                "colname2": ["Carlos", "Ada"],
            }
        )
        expected["colname1"] = expected["colname1"].astype(dtype=pd.Int64Dtype())

        self.chunked_dataframe_values(mock_construct_chunked_dataframe, [expected])

        schema = Schema(
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

        self.s3_adapter.find_schema.return_value = schema

        partitioned_data = [
            ("colname1=1234", pd.DataFrame({"colname2": ["Carlos"]})),
            ("colname1=4567", pd.DataFrame({"colname2": ["Ada"]})),
        ]
        mock_partitioner.return_value = partitioned_data

        self.data_service.generate_raw_filename = Mock(
            return_value=("2022-03-02T12:00:00-data.csv")
        )

        permanent_filename = self.data_service.upload_dataset(
            "some", "other", Path("data.csv"), "data.csv"
        )
        assert permanent_filename == "2022-03-02T12:00:00-data.parquet"

        self.s3_adapter.upload_partitioned_data.assert_called_once_with(
            "some", "other", "2022-03-02T12:00:00-data.parquet", partitioned_data
        )

        self.glue_adapter.update_catalog_table_config.assert_called_once_with(schema)

    # Happy Path -------------------------------------
    @patch("api.application.services.data_service.construct_chunked_dataframe")
    @patch("api.application.services.data_service.generate_partitioned_data")
    def test_upload_dataset_with_overwrite_behaviour_with_no_partition(
        self, mock_partitioner, mock_construct_chunked_dataframe
    ):
        expected = pd.DataFrame(
            {
                "colname1": [1234, 4567],
                "colname2": ["Carlos", "Ada"],
            }
        )
        expected["colname1"] = expected["colname1"].astype(dtype=pd.Int64Dtype())

        self.chunked_dataframe_values(mock_construct_chunked_dataframe, [expected])

        schema = Schema(
            metadata=SchemaMetadata(
                domain="some",
                dataset="other",
                sensitivity="PUBLIC",
                owners=[Owner(name="owner", email="owner@email.com")],
                update_behaviour=UpdateBehaviour.OVERWRITE.value,
            ),
            columns=[
                Column(
                    name="colname1",
                    partition_index=None,
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

        self.s3_adapter.find_schema.return_value = schema

        partitioned_data = [("", expected)]
        mock_partitioner.return_value = partitioned_data

        permanent_filename = self.data_service.upload_dataset(
            "some", "other", Path("data.csv"), "data.csv"
        )
        assert permanent_filename == "some.parquet"

        self.s3_adapter.upload_partitioned_data.assert_called_once_with(
            "some", "other", "some.parquet", partitioned_data
        )

        self.glue_adapter.update_catalog_table_config.assert_called_once_with(schema)

    @patch("api.application.services.data_service.construct_chunked_dataframe")
    @patch("api.application.services.data_service.generate_partitioned_data")
    def test_upload_dataset_with_overwrite_behaviour_with_two_partitions(
        self, mock_partitioner, mock_construct_chunked_dataframe
    ):
        expected = pd.DataFrame(
            {
                "colname1": [1234, 4567],
                "colname2": ["Carlos", "Ada"],
            }
        )
        expected["colname1"] = expected["colname1"].astype(dtype=pd.Int64Dtype())

        self.chunked_dataframe_values(mock_construct_chunked_dataframe, [expected])

        schema = Schema(
            metadata=SchemaMetadata(
                domain="some",
                dataset="other",
                sensitivity="PUBLIC",
                owners=[Owner(name="owner", email="owner@email.com")],
                update_behaviour=UpdateBehaviour.OVERWRITE.value,
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

        self.s3_adapter.find_schema.return_value = schema

        partitioned_data = [
            ("colname1=1234", pd.DataFrame({"colname2": ["Carlos"]})),
            ("colname1=4567", pd.DataFrame({"colname2": ["Ada"]})),
        ]
        mock_partitioner.return_value = partitioned_data

        permanent_filename = self.data_service.upload_dataset(
            "some", "other", Path("data.csv"), "data.csv"
        )
        assert permanent_filename == "some.parquet"

        self.s3_adapter.upload_partitioned_data.assert_called_once_with(
            "some", "other", "some.parquet", partitioned_data
        )

        self.glue_adapter.update_catalog_table_config.assert_called_once_with(schema)

    @patch("api.application.services.data_service.pd")
    def test_construct_chunked_dataframe(self, mock_pd):
        path = Path("file/path")

        construct_chunked_dataframe(path)
        mock_pd.read_csv.assert_called_once_with(
            path, encoding=CONTENT_ENCODING, sep=",", chunksize=1_000_000
        )

    # E2E flow ---------------------------------------
    @patch("api.application.services.data_service.construct_chunked_dataframe")
    @patch("api.application.services.data_service.generate_partitioned_data")
    def test_upload_formatted_dataset_after_transformation_and_validation(
        self, mock_partitioner, mock_construct_chunked_dataframe
    ):
        self.chunked_dataframe_values(
            mock_construct_chunked_dataframe,
            [
                pd.DataFrame(
                    {
                        # Incorrectly formatted column headings
                        # Date format to be transformed
                        # Entirely null row
                        "DaTe": ["12/06/2012", "12/07/2012", pd.NA],
                        "Va!lu-e": ["Carolina", "Albertine", pd.NA],
                    }
                )
            ],
        )

        schema = Schema(
            metadata=SchemaMetadata(
                domain="some",
                dataset="other",
                sensitivity="PUBLIC",
                owners=[Owner(name="owner", email="owner@email.com")],
            ),
            columns=[
                Column(
                    name="date",
                    partition_index=None,
                    data_type="date",
                    format="%d/%m/%Y",
                    allow_null=True,
                ),
                Column(
                    name="value",
                    partition_index=None,
                    data_type="object",
                    allow_null=False,
                ),
            ],
        )

        self.s3_adapter.find_schema.return_value = schema

        expected_transformed_df = pd.DataFrame(
            {
                "date": ["2012-06-12", "2012-07-12"],
                "value": ["Carolina", "Albertine"],
            }
        )

        mock_partitioner.return_value(expected_transformed_df)

        self.data_service.generate_raw_filename = Mock(
            return_value="2022-03-03T12:00:00-data.csv"
        )

        permanent_filename = self.data_service.upload_dataset(
            "some", "other", Path("data.parquet"), "data.parquet"
        )
        assert permanent_filename == "2022-03-03T12:00:00-data.parquet"

        #  Checking mock calls with dataframes does not work because we need to call the df.equals() method
        partitioner_args = mock_partitioner.call_args.args
        upload_args = self.s3_adapter.upload_partitioned_data.call_args.args

        assert partitioner_args[1].equals(expected_transformed_df)

        assert upload_args[0] == "some"
        assert upload_args[1] == "other"
        assert upload_args[2] == "2022-03-03T12:00:00-data.parquet"
        assert upload_args[3].equals(expected_transformed_df)

        self.glue_adapter.update_catalog_table_config.assert_called_once_with(schema)

    # Schema retrieval -------------------------------
    def test_raises_error_when_schema_does_not_exist(self):
        file_contents = set_encoded_content(
            "colname1,colname2\n" "1234,Carlos\n" "4567,Ada\n"
        )

        self.s3_adapter.find_schema.return_value = None

        with pytest.raises(SchemaNotFoundError):
            self.data_service.upload_dataset("some", "other", "data.csv", file_contents)

        self.s3_adapter.find_schema.assert_called_once_with("some", "other", 1)

    # Crawler state and trigger ----------------------
    def test_upload_dataset_fails_when_unable_to_get_crawler_state(self):
        file_contents = set_encoded_content(
            "colname1,colname2\n" "1234,Carlos\n" "4567,Ada\n"
        )

        self.s3_adapter.find_schema.return_value = Schema(
            metadata=SchemaMetadata(
                domain="some",
                dataset="other",
                sensitivity="PUBLIC",
                owners=[Owner(name="owner", email="owner@email.com")],
            ),
            columns=[
                Column(
                    name="colname1",
                    partition_index=None,
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

        self.glue_adapter.check_crawler_is_ready.side_effect = AWSServiceError(
            "Some message"
        )

        with pytest.raises(AWSServiceError, match="Some message"):
            self.data_service.upload_dataset("some", "other", "data.csv", file_contents)

        self.glue_adapter.check_crawler_is_ready.assert_called_once_with(
            "some", "other"
        )

    def test_upload_dataset_fails_when_crawler_is_not_ready(self):
        file_contents = set_encoded_content(
            "colname1,colname2\n" "1234,Carlos\n" "4567,Ada\n"
        )

        self.s3_adapter.find_schema.return_value = Schema(
            metadata=SchemaMetadata(
                domain="some",
                dataset="other",
                sensitivity="PUBLIC",
                owners=[Owner(name="owner", email="owner@email.com")],
            ),
            columns=[
                Column(
                    name="colname1",
                    partition_index=None,
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

        self.glue_adapter.check_crawler_is_ready.side_effect = CrawlerIsNotReadyError(
            "msg"
        )

        with pytest.raises(CrawlerIsNotReadyError):
            self.data_service.upload_dataset("some", "other", "data.csv", file_contents)

        self.glue_adapter.check_crawler_is_ready.assert_called_once_with(
            "some", "other"
        )

    @patch("api.application.services.data_service.construct_chunked_dataframe")
    def test_upload_dataset_starts_crawler(self, mock_construct_chunked_dataframe):
        self.chunked_dataframe_values(
            mock_construct_chunked_dataframe,
            [pd.DataFrame({"colname1": [1234, 4567], "colname2": ["Carlos", "Ada"]})],
        )

        self.s3_adapter.find_schema.return_value = Schema(
            metadata=SchemaMetadata(
                domain="some",
                dataset="other",
                sensitivity="PUBLIC",
                owners=[Owner(name="owner", email="owner@email.com")],
            ),
            columns=[
                Column(
                    name="colname1",
                    partition_index=None,
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

        self.data_service.upload_dataset("some", "other", Path("data.csv"), "data.csv")

        self.glue_adapter.start_crawler.assert_called_once_with("some", "other")

    @patch("api.application.services.data_service.construct_chunked_dataframe")
    def test_upload_dataset_fails_to_start_crawler(
        self, mock_construct_chunked_dataframe
    ):
        self.chunked_dataframe_values(
            mock_construct_chunked_dataframe,
            [pd.DataFrame({"colname1": [1234, 4567], "colname2": ["Carlos", "Ada"]})],
        )

        self.s3_adapter.find_schema.return_value = Schema(
            metadata=SchemaMetadata(
                domain="some",
                dataset="other",
                sensitivity="PUBLIC",
                owners=[Owner(name="owner", email="owner@email.com")],
            ),
            columns=[
                Column(
                    name="colname1",
                    partition_index=None,
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

        self.glue_adapter.start_crawler.side_effect = CrawlerStartFailsError(
            "Some thing"
        )

        with pytest.raises(CrawlerStartFailsError):
            self.data_service.upload_dataset(
                "some", "other", Path("data.csv"), "data.csv"
            )

    # Persist raw copy of data -------------------------------
    @patch("api.application.services.data_service.construct_chunked_dataframe")
    def test_upload_dataset_persists_raw_copy_of_data(
        self, mock_construct_chunked_dataframe
    ):
        self.chunked_dataframe_values(
            mock_construct_chunked_dataframe,
            [pd.DataFrame({"colname1": [1234, 4567], "colname2": ["Carlos", "Ada"]})],
        )

        self.s3_adapter.find_schema.return_value = Schema(
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
        raw_filename = "2022-03-03T12:00:00-data.csv"

        self.data_service.generate_raw_filename = Mock(return_value=(raw_filename))

        self.data_service.upload_dataset("some", "other", Path("data.csv"), "data.csv")

        self.s3_adapter.upload_raw_data.assert_called_once_with(
            "some", "other", Path("data.csv"), raw_filename
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

    # Validation for chunks ---------------------------
    @patch("api.application.services.data_service.construct_chunked_dataframe")
    def test_upload_dataset_in_chunks_with_invalid_column_headers(
        self, mock_construct_chunked_dataframe
    ):
        schema = Schema(
            metadata=SchemaMetadata(
                domain="some",
                dataset="other",
                sensitivity="PUBLIC",
                owners=[Owner(name="owner", email="owner@email.com")],
            ),
            columns=[
                Column(
                    name="colname1",
                    partition_index=None,
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
            self.data_service.upload_dataset(
                "some", "other", Path("data.csv"), "data.csv"
            )

    @patch("api.application.services.data_service.construct_chunked_dataframe")
    def test_upload_dataset_in_chunks_with_invalid_data(
        self, mock_construct_chunked_dataframe
    ):
        schema = Schema(
            metadata=SchemaMetadata(
                domain="some",
                dataset="other",
                sensitivity="PUBLIC",
                owners=[Owner(name="owner", email="owner@email.com")],
            ),
            columns=[
                Column(
                    name="colname1",
                    partition_index=None,
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
            self.data_service.upload_dataset(
                "some", "other", Path("data.csv"), "data.csv"
            )
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
        schema = Schema(
            metadata=SchemaMetadata(
                domain="some",
                dataset="other",
                sensitivity="PUBLIC",
                owners=[Owner(name="owner", email="owner@email.com")],
            ),
            columns=[
                Column(
                    name="colname1",
                    partition_index=None,
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
            self.data_service.upload_dataset(
                "some", "other", Path("data.csv"), "data.csv"
            )
        except DatasetValidationError as error:
            assert {
                "Column [colname1] has an incorrect data type. Expected Int64, received "
                "object",
                "Failed to convert column [colname1] to type [Int64]",
            }.issubset(error.message)


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

    def test_filename_with_timestamp(self):
        filename = self.data_service.generate_raw_filename("data")
        pattern = "\\d{4}-\\d{2}-\\d{2}T\\d{2}\\:\\d{2}\\:\\d{2}-data"
        assert re.match(pattern, filename)
