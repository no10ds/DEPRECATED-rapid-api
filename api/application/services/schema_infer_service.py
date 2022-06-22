from io import StringIO
from typing import List, Union

import pandas as pd

from api.application.services.schema_validation import validate_schema
from api.common.config.constants import CONTENT_ENCODING
from api.common.custom_exceptions import UserError
from api.common.value_transformers import clean_column_name
from api.domain.data_types import DataTypes
from api.domain.schema import Schema, SchemaMetadata, Owner, Column


class SchemaInferService:
    def infer_schema(
        self,
        resource_prefix: str,
        domain: str,
        dataset: str,
        sensitivity: str,
        file_content: Union[bytes, str],
    ) -> Schema:
        dataframe = self._construct_dataframe(file_content)
        schema = Schema(
            metadata=SchemaMetadata(
                resource_prefix=resource_prefix,
                domain=domain,
                dataset=dataset,
                sensitivity=sensitivity,
                owners=[Owner(name="change_me", email="change_me@email.com")],
            ),
            columns=self._infer_columns(dataframe),
        )
        validate_schema(schema)
        return schema

    def transform_to_nullable_data_type(self, data_type_name):
        if data_type_name.capitalize() in DataTypes.numeric_data_types():
            data_type_name = data_type_name.capitalize()
        if data_type_name in "boolean":
            data_type_name = "boolean"
        return data_type_name

    def _construct_dataframe(self, file_content: Union[bytes, str]) -> pd.DataFrame:
        parsed_contents = StringIO(str(file_content, CONTENT_ENCODING))
        try:
            return pd.read_csv(parsed_contents, encoding=CONTENT_ENCODING, sep=",")
        except ValueError as error:
            raise UserError(
                f"The dataset you have provided is not formatted correctly: {self._clean_error(error.args[0])}"
            )

    def _clean_error(self, error_message: str) -> str:
        return error_message.replace("\n", "")

    def _infer_columns(self, dataframe: pd.DataFrame) -> List[Column]:
        columns = []
        for data_column in dataframe.columns:
            columns.append(
                self._infer_column(data_column, dataframe[data_column].dtype)
            )
        return columns

    def _infer_column(self, name: str, data_type) -> Column:
        data_type_name = data_type.name
        data_type_name = self.transform_to_nullable_data_type(data_type_name)
        return Column(
            name=clean_column_name(name),
            partition_index=None,
            data_type=data_type_name,
            allow_null=True,
            format=None,
        )
