from unittest.mock import Mock

from api.adapter.dynamodb_adapter import DynamoDBAdapter
from api.domain.client import ClientResponse


class TestDynamoDBAdapter:
    def setup_method(self):
        self.dynamo_boto_client = Mock()
        self.test_permissions_table_name = 'TEST PERMISSIONS'
        self.dynamo_adapter = DynamoDBAdapter(self.dynamo_boto_client, self.test_permissions_table_name)

    def test_dynamo_db_create_client_item(self):
        test_client_info = ClientResponse(
            client_name='test_client',
            client_id='123456789',
            client_secret='thisisasecret',  # pragma: allowlist secret
            scopes=['READ_PUBLIC', 'READ_PRIVATE']
        )

        self.dynamo_adapter.create_client_item(test_client_info)

        self.dynamo_boto_client.put_item.assert_called_once_with(
            TableName=self.test_permissions_table_name,
            Item={
                "PK": {"S": "USR#Client"},
                "SK": {"S": f"USR#${test_client_info.client_id}"},
                "Id": {"S": f"${test_client_info.client_id}"},
                "Type": {"S": "Client"},
                "Permissions": {"SS": test_client_info.scopes}
            }
        )

    def test_get_existing_scopes_with_db(self):
        expected_scan_response = {
            "Items":
                [{'Sensitivity': {'S': 'ALL'},
                  'SK': {'S': 'PER#1'},
                  'Id': {'S': '1'},
                  'PK': {'S': 'PER#READ'},
                  'Type': {'S': 'READ'}}, {'Sensitivity': {'S': 'ALL'},
                 'SK': {'S': 'PER#2'},
                 'Id': {'S': '2'},
                 'PK': {'S': 'PER#WRITE'},
                 'Type': {'S': 'WRITE'}}],
            'Count': 8, 'ScannedCount': 9,
        }
        self.dynamo_boto_client.scan.return_value = expected_scan_response

        self.dynamo_adapter.get_scope_ids()

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
        # assert response == ["0", "1"]
