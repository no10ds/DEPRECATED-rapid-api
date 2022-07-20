import boto3

from api.common.config.aws import AWS_REGION
from api.domain.client import ClientResponse

from typing import List

from api.domain.permission_item import PermissionItem

PERMISSIONS_TABLE_NAME = 'rapid_users_permissions'


class DynamoDBAdapter:
    def __init__(self,
                 dynamodb_client=boto3.client('dynamodb', region_name=AWS_REGION),
                 permissions_table_name=PERMISSIONS_TABLE_NAME):
        self.dynamodb_client = dynamodb_client
        self.permissions_table_name = permissions_table_name

    def create_client_item(self, client_info: ClientResponse):
        self.dynamodb_client.put_item(
            TableName=self.permissions_table_name,
            Item={
                "PK": {"S": "USR#Client"},
                "SK": {"S": f"USR#${client_info.client_id}"},
                "Id": {"S": f"${client_info.client_id}"},
                "Type": {"S": "Client"},
                "Permissions": {"SS": client_info.scopes}
            }
        )

    def get_scope_ids(self) -> List[PermissionItem]:
        table_items = self.dynamodb_client.scan(
            TableName=self.permissions_table_name,
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

        permissions_list = [self._generate_permission_item(item) for item in table_items['Items']]
        return permissions_list

    def _generate_permission_item(self, item: dict) -> PermissionItem:
        permission = PermissionItem(
            id=item['Id'][list(item['Id'])[0]],
            sensitivity=item['Sensitivity'][list(item['Sensitivity'])[0]],
            type=item['Type'][list(item['Type'])[0]]
        )
        return permission
