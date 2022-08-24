import time
from typing import List, Tuple

import pandas as pd

from api.adapter.athena_adapter import AthenaAdapter
from api.adapter.cognito_adapter import CognitoAdapter
from api.adapter.glue_adapter import GlueAdapter
from api.adapter.s3_adapter import S3Adapter
from api.application.services.dataset_validation import get_validated_dataframe
from api.application.services.partitioning_service import generate_partitioned_data
from api.application.services.protected_domain_service import ProtectedDomainService
from api.application.services.schema_validation import validate_schema_for_upload
from api.common.config.auth import SensitivityLevel
from api.common.custom_exceptions import (
    SchemaNotFoundError,
    ConflictError,
    UserError,
)
from api.common.logger import AppLogger
from api.domain.data_types import DataTypes
from api.domain.enriched_schema import (
    EnrichedSchema,
    EnrichedSchemaMetadata,
    EnrichedColumn,
)
from api.domain.schema import Schema
from api.domain.schema_metadata import UpdateBehaviour
from api.domain.sql_query import SQLQuery
from api.domain.storage_metadata import StorageMetaData


class DataService:
    def __init__(
        self,
        persistence_adapter=S3Adapter(),
        glue_adapter=GlueAdapter(),
        athena_adapter=AthenaAdapter(),
        protected_domain_service=ProtectedDomainService(),
        cognito_adapter=CognitoAdapter(),
    ):
        self.persistence_adapter = persistence_adapter
        self.glue_adapter = glue_adapter
        self.athena_adapter = athena_adapter
        self.protected_domain_service = protected_domain_service
        self.cognito_adapter = cognito_adapter

    def list_raw_files(self, domain: str, dataset: str) -> list[str]:
        raw_files = self.persistence_adapter.list_raw_files(domain, dataset)
        if len(raw_files) == 0:
            raise UserError(
                f"There are no uploaded files for the domain [{domain}] or dataset [{dataset}]"
            )
        else:
            return raw_files

    def generate_raw_filename(self, filename: str) -> str:
        return f'{time.strftime("%Y-%m-%dT%H:%M:%S")}-{filename}'

    def generate_raw_and_permanent_filenames(
        self, schema: Schema, filename: str
    ) -> Tuple[str, str]:
        behaviour = schema.get_update_behaviour()

        raw_filename = self.generate_raw_filename(filename)
        converter = {
            UpdateBehaviour.APPEND.value: f"{raw_filename.replace('.csv', '')}.parquet",
            UpdateBehaviour.OVERWRITE.value: f"{schema.get_domain()}.parquet",
        }
        permanent_filename = converter[behaviour]
        return raw_filename, permanent_filename

    def upload_dataset(
        self,
        domain: str,
        dataset: str,
        filename: str,
        file_contents: bytes,
    ) -> str:
        schema = self._get_schema(domain, dataset)
        if not schema:
            raise SchemaNotFoundError(
                f"Could not find schema related to the dataset [{dataset}]"
            )
        else:
            self.glue_adapter.check_crawler_is_ready(domain, dataset)
            validated_dataframe = get_validated_dataframe(schema, file_contents)
            (
                raw_filename,
                permanent_filename,
            ) = self.generate_raw_and_permanent_filenames(schema, filename)
            self.persistence_adapter.upload_raw_data(
                schema.get_domain(),
                schema.get_dataset(),
                raw_filename,
                file_contents,
            )
            self._upload_data(schema, validated_dataframe, permanent_filename)
            self.glue_adapter.start_crawler(domain, dataset)
            self.glue_adapter.update_catalog_table_config(schema)
            return permanent_filename

    def upload_schema(self, schema: Schema) -> str:
        if self._get_schema(schema.get_domain(), schema.get_dataset()) is not None:
            AppLogger.warning(
                "Schema already exists for domain=%s and dataset=%s",
                schema.get_domain(),
                schema.get_dataset(),
            )
            raise ConflictError("Schema already exists")

        self.check_for_protected_domain(schema)
        validate_schema_for_upload(schema)
        schema_name = self.persistence_adapter.save_schema(
            schema.get_domain(), schema.get_dataset(), schema.get_sensitivity(), schema
        )
        self.glue_adapter.create_crawler(
            schema.get_domain(),
            schema.get_dataset(),
            schema.get_tags(),
        )
        return schema_name

    def check_for_protected_domain(self, schema: Schema):
        if SensitivityLevel.PROTECTED.value == schema.get_sensitivity():
            if (
                schema.get_domain().lower()
                not in self.protected_domain_service.list_protected_domains()
            ):
                raise UserError(
                    f"The protected domain '{schema.get_domain()}' does not exist."
                )
        return schema.get_domain()

    def get_dataset_info(self, domain: str, dataset: str) -> EnrichedSchema:
        schema = self._get_schema(domain, dataset)
        if not schema:
            raise SchemaNotFoundError(
                f"Could not find schema related to the domain [{domain}] and dataset [{dataset}]"
            )
        statistics_dataframe = self.athena_adapter.query(
            domain, dataset, self._build_query(schema)
        )
        last_updated = self.glue_adapter.get_table_last_updated_date(
            StorageMetaData(domain, dataset).glue_table_name()
        )
        return EnrichedSchema(
            metadata=self._enrich_metadata(schema, statistics_dataframe, last_updated),
            columns=self._enrich_columns(schema, statistics_dataframe),
        )

    def _upload_data(
        self, schema: Schema, validated_dataframe: pd.DataFrame, filename: str
    ):
        partitioned_data = generate_partitioned_data(schema, validated_dataframe)
        self.persistence_adapter.upload_partitioned_data(
            schema.get_domain(), schema.get_dataset(), filename, partitioned_data
        )

    def _get_schema(self, domain: str, dataset: str) -> Schema:
        return self.persistence_adapter.find_schema(domain, dataset)

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
