import re
from typing import Callable

import awswrangler as wr
from awswrangler.exceptions import QueryFailed
from botocore.exceptions import ClientError
from pandas import DataFrame

from api.adapter.aws_resource_adapter import AWSResourceAdapter
from api.common.config.aws import ATHENA_DATABASE, OUTPUT_QUERY_BUCKET, ATHENA_WORKGROUP
from api.common.custom_exceptions import UserError
from api.domain.sql_query import SQLQuery
from api.domain.storage_metadata import StorageMetaData


class AthenaAdapter:
    def __init__(
        self,
        database: str = ATHENA_DATABASE,
        s3_output: str = OUTPUT_QUERY_BUCKET,
        workgroup: str = ATHENA_WORKGROUP,
        athena_read_sql_query: Callable[
            [str, str], DataFrame
        ] = wr.athena.read_sql_query,
        aws_resource_adapter=AWSResourceAdapter(),
    ):
        self.aws_resource_adapter = aws_resource_adapter
        self.__database = database
        self.__workgroup = workgroup
        self.__s3_output = s3_output
        self.__athena_read_sql_query = athena_read_sql_query
        self.__default_end_date = "9999-12-01"

    def query(
        self, domain: str, dataset: str, version: int, query: SQLQuery
    ) -> DataFrame:
        if version == -1:
            version = self.aws_resource_adapter.get_version_from_crawler_tags(
                domain, dataset
            )
        table_name = StorageMetaData(domain, dataset, version).glue_table_name()
        try:
            return self.__athena_read_sql_query(
                sql=query.to_sql(table_name),
                database=self.__database,
                ctas_approach=False,
                workgroup=self.__workgroup,
                s3_output=self.__s3_output,
            )
        except QueryFailed as error:
            self._handle_query_error(error, table_name)
        except ClientError as error:
            self._handle_client_error(error)

    def _handle_client_error(self, error):
        if error.response["Error"]["Code"] == "InvalidRequestException":
            raise UserError(f'Failed to execute query: {error.response["Message"]}')

    def _handle_query_error(self, error, table_name):
        if re.match(".+ Table .+ does not exist", error.args[0]):
            raise UserError(
                f"Query failed to execute: The table [{table_name}] does not exist. The data could be currently processing or you might need to upload it."
            )
        raise UserError(f"Query failed to execute: {error.args[0]}")
