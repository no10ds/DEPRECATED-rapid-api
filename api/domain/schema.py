from typing import List, Dict, Optional, Set

from pydantic.main import BaseModel

from api.domain.data_types import DataTypes
from api.domain.schema_metadata import Owner, SchemaMetadata, UpdateBehaviour


class Column(BaseModel):
    name: str
    partition_index: Optional[int]
    data_type: str
    allow_null: bool
    format: Optional[str] = None


class Schema(BaseModel):
    metadata: SchemaMetadata
    columns: List[Column]

    def get_layer(self) -> str:
        return self.metadata.get_layer()

    def get_domain(self) -> str:
        return self.metadata.get_domain().lower()

    def get_dataset(self) -> str:
        return self.metadata.get_dataset()

    def get_description(self) -> str:
        return self.metadata.get_description()

    def get_sensitivity(self) -> str:
        return self.metadata.get_sensitivity()

    def get_version(self) -> int:
        return self.metadata.get_version()

    def get_custom_tags(self) -> Dict[str, str]:
        return self.metadata.get_custom_tags()

    def get_tags(self) -> Dict[str, str]:
        return self.metadata.get_tags()

    def get_owners(self) -> Optional[List[Owner]]:
        return self.metadata.get_owners()

    def get_update_behaviour(self) -> str:
        return self.metadata.get_update_behaviour()

    def has_overwrite_behaviour(self) -> bool:
        return self.get_update_behaviour() == UpdateBehaviour.OVERWRITE

    def get_column_names(self) -> List[str]:
        return [column.name for column in self.columns]

    # TODO: Is this necessary in athena? Might it do it for us?
    # def get_column_dtypes_to_cast(self) -> Dict[str, str]:
    #     return {
    #         column.name: column.data_type
    #         for column in self.columns
    #         if column.data_type in DataTypes.data_types_to_cast()
    #     }

    def get_partitions(self) -> List[str]:
        sorted_cols = self.get_partition_columns()
        return [column.name for column in sorted_cols]

    def get_partition_indexes(self) -> List[int]:
        sorted_cols = self.get_partition_columns()
        return [column.partition_index for column in sorted_cols]

    def get_data_types(self) -> Set[str]:
        return {column.data_type for column in self.columns}

    def get_columns_by_type(self, d_type: str) -> List[Column]:
        return [column for column in self.columns if column.data_type == d_type]

    def get_column_names_by_type(self, d_type: str) -> List[str]:
        return [column.name for column in self.columns if column.data_type == d_type]

    def get_non_partition_columns_for_glue(self) -> List[dict]:
        return [
            self.convert_column_to_glue_format(col)
            for col in self.columns
            if col.partition_index is None
        ]

    def get_partition_columns_for_glue(self) -> List[dict]:
        return [
            self.convert_column_to_glue_format(col)
            for col in self.get_partition_columns()
        ]

    def convert_column_to_glue_format(self, column: List[Column]):
        return {"Name": column.name, "Type": column.data_type}

    def get_partition_columns(self) -> List[Column]:
        return sorted(
            [column for column in self.columns if column.partition_index is not None],
            key=lambda x: x.partition_index,
        )

    def get_non_partition_column_types(self) -> dict:
        return {
            col.name: col.data_type
            for col in self.columns
            if col.partition_index is None
        }

    def get_partition_column_types(self) -> dict:
        return {col.name: col.data_type for col in self.get_partition_columns()}
