from unittest.mock import Mock

import pytest

from api.adapter.dynamodb_adapter import DynamoDBAdapter
from api.common.custom_exceptions import UserError
from api.domain.client import ClientResponse


class TestDynamoDBAdapter:
    expected_db_scan_response = {
        "Items":
            [
                {'SK': {'S': 'PER#0'},
                 'Id': {'S': '0'},
                 'PK': {'S': 'PER#USER_ADMIN'},
                 'Type': {'S': 'USER_ADMIN'}
                 },
                {'Sensitivity': {'S': 'ALL'},
                 'SK': {'S': 'PER#1'},
                 'Id': {'S': '1'},
                 'PK': {'S': 'PER#READ'},
                 'Type': {'S': 'READ'}},
                {'Sensitivity': {'S': 'ALL'},
                 'SK': {'S': 'PER#2'},
                 'Id': {'S': '2'},
                 'PK': {'S': 'PER#WRITE'},
                 'Type': {'S': 'WRITE'}
                 },
                {'Sensitivity': {'S': 'PRIVATE'},
                 'SK': {'S': 'PER#3'},
                 'Id': {'S': '3'},
                 'PK': {'S': 'PER#READ'},
                 'Type': {'S': 'READ'}
                 }
            ],
        'Count': 4, 'ScannedCount': 5,
    }

    def setup_method(self):
        self.dynamo_boto_client = Mock()
        self.test_permissions_table_name = 'TEST PERMISSIONS'
        self.dynamo_adapter = DynamoDBAdapter(self.dynamo_boto_client, self.test_permissions_table_name)

    def test_dynamo_db_create_client_item(self):
        test_client_info = ClientResponse(
            client_name='test_client',
            client_id='123456789',
            client_secret='thisisasecret',  # pragma: allowlist secret
            scopes=["READ_ALL", "WRITE_ALL"]
        )
        self.dynamo_boto_client.scan.return_value = self.expected_db_scan_response
        self.dynamo_adapter.create_client_item(test_client_info)

        self.dynamo_boto_client.put_item.assert_called_once_with(
            TableName=self.test_permissions_table_name,
            Item={
                "PK": {"S": "USR#CLIENT"},
                "SK": {"S": f"USR#${test_client_info.client_id}"},
                "Id": {"S": f"${test_client_info.client_id}"},
                "Type": {"S": "CLIENT"},
                "Permissions": {"SS": ["1", "2"]}
            }
        )

    def test_get_existing_scopes_for_user_with_db(self):
        test_user_permissions = ["READ_ALL", "WRITE_ALL"]
        self.dynamo_boto_client.scan.return_value = self.expected_db_scan_response

        response = self.dynamo_adapter.get_scope_ids(test_user_permissions)

        self.dynamo_boto_client.scan.assert_called_once_with(
            TableName=self.test_permissions_table_name,
            ScanFilter={
                'PK': {
                    'AttributeValueList': [
                        {
                            'S': 'PER',
                        },
                    ],
                    'ComparisonOperator': 'BEGINS_WITH'
                }
            },
        )
        assert response == ["1", "2"]

    def test_get_existing_scopes_for_user_with_no_permissions_from_db(self):
        test_user_permissions = []
        self.dynamo_boto_client.scan.return_value = self.expected_db_scan_response

        response = self.dynamo_adapter.get_scope_ids(test_user_permissions)

        self.dynamo_boto_client.scan.assert_called_once_with(
            TableName=self.test_permissions_table_name,
            ScanFilter={
                'PK': {
                    'AttributeValueList': [
                        {
                            'S': 'PER',
                        },
                    ],
                    'ComparisonOperator': 'BEGINS_WITH'
                }
            },
        )
        assert response == []

    def test_get_existing_scopes_for_user_with_all_invalid_permissions_from_db(self):
        test_user_permissions = ["READ_SENSITIVE", "ACCESS_ALL", "ADMIN", "FAKE_ADMIN"]

        self.dynamo_boto_client.scan.return_value = self.expected_db_scan_response

        with pytest.raises(
                UserError, match="One or more of the provided scopes do not exist"
        ):
            self.dynamo_adapter.get_scope_ids(test_user_permissions)

    def test_get_existing_scopes_for_user_with_some_invalid_permissions_from_db(self):
        test_user_permissions = ["READ_SENSITIVE", "WRITE_ALL", "ACCESS_ALL", "ADMIN", "FAKE_ADMIN"]

        self.dynamo_boto_client.scan.return_value = self.expected_db_scan_response

        with pytest.raises(
                UserError, match="One or more of the provided scopes do not exist"
        ):
            self.dynamo_adapter.get_scope_ids(test_user_permissions)

    def test_get_existing_scopes_with_db_with_user_admin_permission(self):
        test_user_permissions = ["USER_ADMIN", "WRITE_ALL"]
        self.dynamo_boto_client.scan.return_value = self.expected_db_scan_response

        response = self.dynamo_adapter.get_scope_ids(test_user_permissions)

        self.dynamo_boto_client.scan.assert_called_once_with(
            TableName=self.test_permissions_table_name,
            ScanFilter={
                'PK': {
                    'AttributeValueList': [
                        {
                            'S': 'PER',
                        },
                    ],
                    'ComparisonOperator': 'BEGINS_WITH'
                }
            },
        )
        assert response == ["0", "2"]
