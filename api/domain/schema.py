from dataclasses import dataclass
from typing import List, Dict, Optional, Set

from pydantic import EmailStr
from pydantic.main import BaseModel

from api.common.config.auth import SensitivityLevel
from api.common.config.aws import SCHEMAS_LOCATION
from api.common.custom_exceptions import SchemaNotFoundError
from api.common.data_parsers import parse_categorisation
from api.common.utilities import BaseEnum
from api.domain.data_types import DataTypes


class Owner(BaseModel):
    name: str
    email: EmailStr


class UpdateBehaviour(BaseEnum):
    APPEND = "APPEND"
    OVERWRITE = "OVERWRITE"


class SchemaMetadata(BaseModel):
    domain: str
    dataset: str
    sensitivity: str
    key_value_tags: Dict[str, str] = dict()
    key_only_tags: List[str] = list()
    owners: Optional[List[Owner]] = None
    update_behaviour: str = UpdateBehaviour.APPEND.value

    def get_domain(self) -> str:
        return self.domain

    def get_dataset(self) -> str:
        return self.dataset

    def get_sensitivity(self) -> str:
        return self.sensitivity

    def schema_path(self) -> str:
        return f"{SCHEMAS_LOCATION}/{self.sensitivity}/{self.schema_name()}"

    def schema_name(self) -> str:
        return f"{self.domain}-{self.dataset}.json"

    @classmethod
    def from_path(cls, path: str):
        sensitivity = parse_categorisation(path, SensitivityLevel.values(), "PUBLIC")
        domain, dataset = path.split("/")[-1].replace(".json", "").split("-")
        return cls(domain=domain, dataset=dataset, sensitivity=sensitivity)

    def get_custom_tags(self) -> Dict[str, str]:
        return {**self.key_value_tags, **dict.fromkeys(self.key_only_tags, "")}

    def get_tags(self) -> Dict[str, str]:
        return {**self.get_custom_tags(), "sensitivity": self.get_sensitivity()}

    def get_owners(self) -> Optional[List[Owner]]:
        return self.owners

    def get_update_behaviour(self) -> str:
        return self.update_behaviour

    def remove_duplicates(self):
        updated_key_only_list = []

        if len(self.key_only_tags) != 0 and self.key_value_tags:
            for key in self.key_only_tags:
                if key not in self.key_value_tags.keys():
                    updated_key_only_list.append(key)

        self.key_only_tags = updated_key_only_list


@dataclass
class SchemaMetadatas:
    metadatas: List[SchemaMetadata]

    def find(self, domain: str, dataset: str) -> SchemaMetadata:
        try:
            return list(
                filter(
                    lambda data: data.domain == domain and data.dataset == dataset,
                    self.metadatas,
                )
            )[0]
        except IndexError:
            raise SchemaNotFoundError(
                f"Schema not found for domain={domain} and dataset={dataset}"
            )

    @classmethod
    def empty(cls):
        return cls([])


class Column(BaseModel):
    name: str
    partition_index: Optional[int]
    data_type: str
    allow_null: bool
    format: Optional[str] = None


class Schema(BaseModel):
    metadata: SchemaMetadata
    columns: List[Column]

    def get_domain(self) -> str:
        return self.metadata.get_domain()

    def get_dataset(self) -> str:
        return self.metadata.get_dataset()

    def get_sensitivity(self) -> str:
        return self.metadata.get_sensitivity()

    def get_custom_tags(self) -> Dict[str, str]:
        return self.metadata.get_custom_tags()

    def get_tags(self) -> Dict[str, str]:
        return self.metadata.get_tags()

    def get_owners(self) -> Optional[List[Owner]]:
        return self.metadata.get_owners()

    def get_update_behaviour(self) -> str:
        return self.metadata.get_update_behaviour()

    def get_column_names(self) -> List[str]:
        return [column.name for column in self.columns]

    def get_column_dtypes_to_cast(self) -> Dict[str, str]:
        return {
            column.name: column.data_type
            for column in self.columns
            if column.data_type in DataTypes.data_types_to_cast()
        }

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

    def get_partition_columns(self) -> List[Column]:
        return sorted(
            [column for column in self.columns if column.partition_index is not None],
            key=lambda x: x.partition_index,
        )

    def get_statistics_query_columns(self) -> List[str]:
        query_columns = []
        for column in self.get_columns_by_type(DataTypes.DATE):
            name = column.name
            query_columns.append(f"max({name}) as max_{name}")
            query_columns.append(f"min({name}) as min_{name}")
        return query_columns
