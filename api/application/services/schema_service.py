from typing import List, Type

from api.adapter.dynamodb_adapter import DynamoDBAdapter
from api.common.config.constants import FIRST_SCHEMA_VERSION_NUMBER
from api.common.custom_exceptions import SchemaNotFoundError
from api.domain.schema import Schema
from api.domain.schema_metadata import SchemaMetadata
from api.domain.dataset_metadata import DatasetMetadata

from api.domain.dataset_filters import DatasetFilters


class SchemaService:
    def __init__(self, dynamodb_adapter=DynamoDBAdapter()):
        self.dynamodb_adapter = dynamodb_adapter

    def store_schema(self, schema: Schema):
        return self.dynamodb_adapter.store_schema(schema)

    def get_schema(
        self, dataset: Type[DatasetMetadata], latest: bool = False
    ) -> Schema:
        if latest:
            schema = self.dynamodb_adapter.get_latest_schema(dataset)
        else:
            schema = self.dynamodb_adapter.get_schema(dataset)

        if not schema:
            raise SchemaNotFoundError(
                f"Could not find the schema for dataset {dataset.string_representation()}"
            )
        return schema

    def get_schemas(
        self, query: DatasetFilters = DatasetFilters()
    ) -> List[SchemaMetadata]:
        return self.dynamodb_adapter.get_latest_schema_metadatas(query)

    def get_latest_schema_version(self, dataset: Type[DatasetMetadata]) -> int:
        schema = self.dynamodb_adapter.get_latest_schema(dataset)
        if not schema:
            return FIRST_SCHEMA_VERSION_NUMBER

        return schema.get_version()

    def deprecate_schema(self, dataset: Type[DatasetMetadata]) -> int:
        return self.dynamodb_adapter.deprecate_schema(dataset)

    def delete_schemas(self, dataset: Type[DatasetMetadata]) -> int:
        dataset.version = self.get_latest_schema_version(dataset)
        return self.dynamodb_adapter.delete_schemas(dataset)
