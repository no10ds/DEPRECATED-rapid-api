import os
import uuid
from pathlib import Path
from threading import Thread
from time import sleep
from typing import List, Tuple

import pandas as pd
from pandas.io.parsers import TextFileReader

from api.adapter.athena_adapter import AthenaAdapter
from api.adapter.aws_resource_adapter import AWSResourceAdapter
from api.adapter.cognito_adapter import CognitoAdapter
from api.adapter.glue_adapter import GlueAdapter
from api.adapter.s3_adapter import S3Adapter
from api.application.services.dataset_validation import build_validated_dataframe
from api.application.services.delete_service import DeleteService
from api.application.services.job_service import JobService
from api.application.services.partitioning_service import generate_partitioned_data
from api.application.services.protected_domain_service import ProtectedDomainService
from api.application.services.schema_validation import validate_schema_for_upload
from api.common.config.auth import SensitivityLevel
from api.common.config.constants import (
    CONTENT_ENCODING,
    DATASET_QUERY_LIMIT,
    FIRST_SCHEMA_VERSION_NUMBER,
    SCHEMA_VERSION_INCREMENT,
)
from api.common.custom_exceptions import (
    AWSServiceError,
    ConflictError,
    CrawlerIsNotReadyError,
    CrawlerUpdateError,
    DatasetValidationError,
    QueryExecutionError,
    SchemaNotFoundError,
    UnprocessableDatasetError,
    UserError,
)
from api.common.logger import AppLogger
from api.common.utilities import build_error_message_list
from api.domain.data_types import DataTypes
from api.domain.dataset_metadata import DatasetMetadata
from api.domain.enriched_schema import (
    EnrichedColumn,
    EnrichedSchema,
    EnrichedSchemaMetadata,
)
from api.domain.Jobs.QueryJob import QueryJob, QueryStep
from api.domain.Jobs.UploadJob import UploadJob, UploadStep
from api.domain.schema import Schema
from api.domain.sql_query import SQLQuery


def construct_chunked_dataframe(file_path: Path) -> TextFileReader:
    return pd.read_csv(file_path, encoding=CONTENT_ENCODING, sep=",", chunksize=200_000)


class DataService:
    def __init__(
        self,
        s3_adapter=S3Adapter(),
        glue_adapter=GlueAdapter(),
        athena_adapter=AthenaAdapter(),
        protected_domain_service=ProtectedDomainService(),
        cognito_adapter=CognitoAdapter(),
        delete_service=DeleteService(),
        job_service=JobService(),
        aws_resource_adapter=AWSResourceAdapter(),
    ):
        self.s3_adapter = s3_adapter
        self.glue_adapter = glue_adapter
        self.athena_adapter = athena_adapter
        self.protected_domain_service = protected_domain_service
        self.cognito_adapter = cognito_adapter
        self.delete_service = delete_service
        self.job_service = job_service
        self.aws_resource_adapter = aws_resource_adapter

    def list_raw_files(self, dataset: DatasetMetadata) -> list[str]:
        raw_files = self.s3_adapter.list_raw_files(dataset)
        if len(raw_files) == 0:
            raise UserError(
                f"There are no uploaded files for the {dataset.string_representation()}"
            )
        else:
            return raw_files

    def generate_raw_file_identifier(self) -> str:
        return str(uuid.uuid4())

    def generate_permanent_filename(self, raw_file_identifier: str) -> str:
        return f"{raw_file_identifier}_{uuid.uuid4()}.parquet"

    def upload_dataset(
        self,
        subject_id: str,
        job_id: str,
        dataset: DatasetMetadata,
        file_path: Path,
    ) -> Tuple[str, int, str]:
        schema = self._get_schema(dataset)
        if not schema:
            raise SchemaNotFoundError(
                f"Could not find schema related to the {dataset.string_representation()}"
            )
        else:
            raw_file_identifier = self.generate_raw_file_identifier()
            upload_job = self.job_service.create_upload_job(
                subject_id, job_id, file_path.name, raw_file_identifier, dataset
            )

            Thread(
                target=self.process_upload,
                args=(upload_job, schema, file_path, raw_file_identifier),
                name=upload_job.job_id,
            ).start()

            return f"{raw_file_identifier}.csv", dataset.version, upload_job.job_id

    def process_upload(
        self, job: UploadJob, schema: Schema, file_path: Path, raw_file_identifier: str
    ) -> None:
        try:
            self.job_service.update_step(job, UploadStep.INITIALISATION)
            self.wait_until_crawler_is_ready(schema.get_dataset_metadata())
            self.job_service.update_step(job, UploadStep.VALIDATION)
            self.validate_incoming_data(schema, file_path, raw_file_identifier)
            self.job_service.update_step(job, UploadStep.RAW_DATA_UPLOAD)
            self.s3_adapter.upload_raw_data(
                schema.metadata, file_path, raw_file_identifier
            )
            self.job_service.update_step(job, UploadStep.DATA_UPLOAD)
            self.process_chunks(schema, file_path, raw_file_identifier)
            self.job_service.update_step(job, UploadStep.CLEAN_UP)
            self.delete_incoming_raw_file(schema, file_path, raw_file_identifier)
            self.job_service.update_step(job, UploadStep.NONE)
            self.job_service.succeed(job)
        except Exception as error:
            AppLogger.error(
                f"Processing upload failed for layer [{schema.get_layer()}], domain [{schema.get_domain()}], dataset [{schema.get_dataset()}], and version [{schema.get_version()}]: {error}"
            )
            self.delete_incoming_raw_file(schema, file_path, raw_file_identifier)
            self.job_service.fail(job, build_error_message_list(error))
            raise error

    def wait_until_crawler_is_ready(self, dataset: DatasetMetadata) -> None:
        remaining_retries = 20
        while remaining_retries > 0:
            try:
                self.glue_adapter.check_crawler_is_ready(dataset)
                return
            except CrawlerIsNotReadyError as error:
                remaining_retries -= 1
                if remaining_retries == 0:
                    raise error
                sleep(30)

    def validate_incoming_data(
        self, schema: Schema, file_path: Path, raw_file_identifier: str
    ) -> None:
        AppLogger.info(
            f"Validating dataset for {schema.get_layer()}/{schema.get_domain()}/{schema.get_dataset()}"
        )
        dataset_errors = set()
        for chunk in construct_chunked_dataframe(file_path):
            try:
                build_validated_dataframe(schema, chunk)
            except DatasetValidationError as error:
                dataset_errors.update(error.message)
        if dataset_errors:
            self.delete_incoming_raw_file(schema, file_path, raw_file_identifier)
            raise DatasetValidationError(list(dataset_errors))

    def process_chunks(
        self, schema: Schema, file_path: Path, raw_file_identifier: str
    ) -> None:
        AppLogger.info(
            f"Processing chunks for {schema.get_layer()}/{schema.get_domain()}/{schema.get_dataset()}/{schema.get_version()}"
        )
        for chunk in construct_chunked_dataframe(file_path):
            self.process_chunk(schema, raw_file_identifier, chunk)

        if schema.has_overwrite_behaviour():
            self.remove_existing_data(schema, raw_file_identifier)

        self.glue_adapter.start_crawler(schema.get_dataset_metadata())
        self.glue_adapter.update_catalog_table_config(schema)
        AppLogger.info(
            f"Processing chunks for {schema.get_layer()}/{schema.get_domain()}/{schema.get_dataset()}/{schema.get_version()} completed"
        )

    def process_chunk(
        self, schema: Schema, raw_file_identifier: str, chunk: pd.DataFrame
    ) -> None:
        validated_dataframe = build_validated_dataframe(schema, chunk)
        permanent_filename = self.generate_permanent_filename(raw_file_identifier)
        self.upload_data(schema, validated_dataframe, permanent_filename)

    def delete_incoming_raw_file(
        self, schema: Schema, file_path: Path, raw_file_identifier: str
    ):
        try:
            os.remove(file_path.name)
            AppLogger.info(
                f"Temporary upload file for {schema.get_layer()}/{schema.get_domain()}/{schema.get_dataset()}/{schema.get_version()} deleted. Raw file identifier: {raw_file_identifier}"
            )
        except (FileNotFoundError, TypeError) as error:
            AppLogger.error(
                f"Temporary upload file for {schema.get_layer()}/{schema.get_domain()}/{schema.get_dataset()}/{schema.get_version()} not deleted. Raw file identifier: {raw_file_identifier}. Detail: {error}"
            )

    def remove_existing_data(self, schema: Schema, raw_file_identifier: str) -> None:
        AppLogger.info(
            f"Overwriting existing data for layer [{schema.get_layer()}], domain [{schema.get_domain()}] and dataset [{schema.get_dataset()}]"
        )
        try:
            self.s3_adapter.delete_previous_dataset_files(
                schema.get_dataset_metadata(),
                raw_file_identifier,
            )
        except IndexError:
            AppLogger.warning(
                f"No data to override for domain [{schema.get_domain()}] and dataset [{schema.get_dataset()}]"
            )
        except AWSServiceError as error:
            AppLogger.error(
                f"Overriding existing data failed for layer [{schema.get_layer()}], domain [{schema.get_domain()}] and dataset [{schema.get_dataset()}]. Raw file identifier: {raw_file_identifier}. {error}"
            )
            raise AWSServiceError(
                f"Overriding existing data failed for layer [{schema.get_layer()}], domain [{schema.get_domain()}] and dataset [{schema.get_dataset()}]. Raw file identifier: {raw_file_identifier}"
            )

    def upload_schema(self, schema: Schema) -> str:
        schema.metadata.version = FIRST_SCHEMA_VERSION_NUMBER
        schema.metadata.domain = schema.metadata.domain.lower()
        if self._get_schema(schema.get_dataset_metadata()) is not None:
            AppLogger.warning(
                "Schema already exists for layer=%s domain=%s dataset=%s and version=%s",
                schema.get_layer(),
                schema.get_domain(),
                schema.get_dataset(),
                schema.get_version(),
            )
            raise ConflictError("Schema already exists")

        self.check_for_protected_domain(schema)
        validate_schema_for_upload(schema)
        schema_name = self.s3_adapter.save_schema(schema)
        self.glue_adapter.create_crawler(
            schema.get_dataset_metadata(), schema.get_tags()
        )
        return schema_name

    def update_schema(self, schema: Schema) -> str:
        try:
            dataset_metadata = DatasetMetadata(
                schema.get_layer(),
                schema.get_domain(),
                schema.get_dataset(),
                FIRST_SCHEMA_VERSION_NUMBER,
            )
            original_schema = self._get_schema(dataset_metadata)
            if original_schema is None:
                AppLogger.warning(
                    f"Could not find schema for {dataset_metadata.string_representation()}"
                )
                raise SchemaNotFoundError("Previous version of schema not found")

            new_schema_description = schema.metadata.description
            new_version = (
                dataset_metadata.get_latest_version(self.aws_resource_adapter)
                + SCHEMA_VERSION_INCREMENT
            )
            schema.metadata = original_schema.metadata
            schema.metadata.version = new_version
            schema.metadata.description = new_schema_description
            self.check_for_protected_domain(schema)
            self.glue_adapter.check_crawler_is_ready(schema.get_dataset_metadata())
            validate_schema_for_upload(schema)

            schema_name = self.s3_adapter.save_schema(schema)
            self.glue_adapter.set_crawler_version_tag(schema.get_dataset_metadata())
            return schema_name
        except CrawlerUpdateError as error:
            self.delete_service.delete_schema(schema.metadata)
            raise error

    def check_for_protected_domain(self, schema: Schema) -> str:
        if SensitivityLevel.PROTECTED.value == schema.get_sensitivity():
            if (
                schema.get_domain()
                not in self.protected_domain_service.list_protected_domains()
            ):
                raise UserError(
                    f"The protected domain '{schema.get_domain()}' does not exist."
                )
        return schema.get_domain()

    def get_dataset_info(self, dataset: DatasetMetadata) -> EnrichedSchema:
        schema = self._get_schema(dataset)
        if not schema:
            raise SchemaNotFoundError(
                f"Could not find schema related to the {dataset.string_representation()}"
            )
        statistics_dataframe = self.athena_adapter.query(
            dataset, self._build_query(schema)
        )
        last_updated = self.glue_adapter.get_table_last_updated_date(
            dataset.glue_table_name()
        )
        return EnrichedSchema(
            metadata=self._enrich_metadata(schema, statistics_dataframe, last_updated),
            columns=self._enrich_columns(schema, statistics_dataframe),
        )

    def upload_data(
        self, schema: Schema, validated_dataframe: pd.DataFrame, filename: str
    ):
        partitioned_data = generate_partitioned_data(schema, validated_dataframe)
        self.s3_adapter.upload_partitioned_data(
            schema.get_dataset_metadata(),
            filename,
            partitioned_data,
        )

    def is_query_too_large(self, dataset: DatasetMetadata, query: SQLQuery):
        if query.limit:
            if int(query.limit) <= DATASET_QUERY_LIMIT:
                return False

        no_of_rows_in_table = self.glue_adapter.get_no_of_rows(
            dataset.glue_table_name()
        )
        return no_of_rows_in_table > DATASET_QUERY_LIMIT

    def query_data(
        self,
        dataset: DatasetMetadata,
        query: SQLQuery,
    ) -> pd.DataFrame:
        if not self.is_query_too_large(dataset, query):
            return self.athena_adapter.query(dataset, query)
        else:
            raise UnprocessableDatasetError("Dataset too large")

    def query_large_data(
        self,
        subject_id: str,
        dataset: DatasetMetadata,
        query: SQLQuery,
    ) -> str:
        query_job = self.job_service.create_query_job(subject_id, dataset)
        query_execution_id = self.athena_adapter.query_async(dataset, query)
        Thread(
            target=self.generate_results_download_url_async,
            args=(
                query_job,
                query_execution_id,
            ),
        ).start()
        return query_job.job_id

    def generate_results_download_url_async(
        self, query_job: QueryJob, query_execution_id: str
    ) -> None:
        try:
            self.job_service.update_step(query_job, QueryStep.RUNNING)
            self.athena_adapter.wait_for_query_to_complete(query_execution_id)
            self.job_service.update_step(query_job, QueryStep.GENERATING_RESULTS)
            url = self.s3_adapter.generate_query_result_download_url(query_execution_id)
            self.job_service.succeed_query(query_job, url)
        except QueryExecutionError as error:
            self.job_service.fail(query_job, build_error_message_list(error))
        except (AWSServiceError, Exception) as error:
            AppLogger.error(f"Large query failed: {error}")
            self.job_service.fail(query_job, build_error_message_list(error))
            raise error

    def update_table_config(self, dataset: DatasetMetadata) -> None:
        schema = self.s3_adapter.find_schema(dataset)
        self.glue_adapter.update_catalog_table_config(schema)

    def _get_schema(self, dataset: DatasetMetadata) -> Schema:
        return self.s3_adapter.find_schema(dataset)

    def _build_query(self, schema: Schema) -> SQLQuery:
        date_columns = schema.get_columns_by_type(DataTypes.DATE)
        date_range_queries = [
            *[f"max({column.name}) as max_{column.name}" for column in date_columns],
            *[f"min({column.name}) as min_{column.name}" for column in date_columns],
        ]
        columns_to_query = [
            "count(*) as data_size",
            *date_range_queries,
        ]
        return SQLQuery(select_columns=columns_to_query)

    def _enrich_metadata(
        self, schema: Schema, statistics_dataframe: pd.DataFrame, last_updated: str
    ) -> EnrichedSchemaMetadata:
        dataset_size = statistics_dataframe.at[0, "data_size"]
        return EnrichedSchemaMetadata(
            **schema.metadata.dict(),
            number_of_rows=dataset_size,
            number_of_columns=len(schema.columns),
            last_updated=last_updated,
        )

    def _enrich_columns(
        self, schema: Schema, statistics_dataframe: pd.DataFrame
    ) -> List[EnrichedColumn]:
        enriched_columns = []
        date_column_names = schema.get_column_names_by_type("date")
        for column in schema.columns:
            statistics = None
            if column.name in date_column_names:
                statistics = {
                    "max": statistics_dataframe.at[0, f"max_{column.name}"],
                    "min": statistics_dataframe.at[0, f"min_{column.name}"],
                }
            enriched_columns.append(
                EnrichedColumn(**column.dict(), statistics=statistics)
            )
        return enriched_columns
