from unittest.mock import Mock

import pandas as pd
import pytest
from awswrangler.exceptions import QueryFailed
from botocore.exceptions import ClientError

from api.common.custom_exceptions import UserError
from api.adapter.athena_adapter import DatasetQuery
from api.domain.sql_query import SQLQuery, SQLQueryOrderBy


class TestAthenaAdapter:
    def setup_method(self):
        self.mock_athena_read_sql_query = Mock()
        self.dataset_query = DatasetQuery(
            database="my_database",
            athena_read_sql_query=self.mock_athena_read_sql_query,
            s3_output="out",
        )

    def test_returns_query_result_dataframe(self):
        query_result_df = pd.DataFrame({"column1": [1, 2], "column2": ["item1", "item2"]})

        self.mock_athena_read_sql_query.return_value = query_result_df

        result = self.dataset_query.query("my", "table", SQLQuery())

        self.mock_athena_read_sql_query.assert_called_once_with(
            sql="SELECT * FROM my_table",
            database="my_database",
            ctas_approach=False,
            workgroup="rapid_athena_workgroup",
            s3_output="out",
        )

        assert result.equals(query_result_df)

    def test_no_query_provided(self):
        self.dataset_query.query("my", "table", SQLQuery())

        self.mock_athena_read_sql_query.assert_called_once_with(
            sql="SELECT * FROM my_table",
            database="my_database",
            ctas_approach=False,
            workgroup="rapid_athena_workgroup",
            s3_output="out",
        )

    def test_query_provided(self):
        self.dataset_query.query("my", "table",
                                 SQLQuery(
                                     select_columns=["column1", "column2"],
                                     group_by_columns=["column2"],
                                     order_by_columns=[SQLQueryOrderBy(column="column1")],
                                     limit=2
                                 ))

        self.mock_athena_read_sql_query.assert_called_once_with(
            sql="SELECT column1,column2 FROM my_table GROUP BY column2 ORDER BY column1 ASC LIMIT 2",
            database="my_database",
            ctas_approach=False,
            workgroup="rapid_athena_workgroup",
            s3_output="out",
        )

    def test_query_fails(self):
        self.mock_athena_read_sql_query.side_effect = QueryFailed("Some error")

        with pytest.raises(UserError, match='Query failed to execute: Some error'):
            self.dataset_query.query("my", "table", SQLQuery())

    def test_query_fails_because_of_invalid_format(self):
        self.mock_athena_read_sql_query.side_effect = ClientError(
            error_response={
                "Error": {"Code": 'InvalidRequestException'},
                "Message": "Failed to execute query: The error message"
            },
            operation_name="StartQueryExecution"
        )

        with pytest.raises(UserError, match="Failed to execute query: The error message"):
            self.dataset_query.query("my", "table", SQLQuery())

    def test_query_fails_because_table_does_not_exist(self):
        self.mock_athena_read_sql_query.side_effect = QueryFailed(
            "SYNTAX_ERROR: line 1:15: Table awsdatacatalog.rapid_catalogue_db.my_table does not exist"
        )

        expected_message = r'Query failed to execute: The table \[my_table\] does not exist. Please check your spelling or there may be no data yet'

        with pytest.raises(UserError, match=expected_message):
            self.dataset_query.query("my", "table", SQLQuery())
