from typing import List

import boto3

from api.common.config.aws import AWS_REGION
from api.common.custom_exceptions import UserError
from api.domain.permission_item import PermissionItem

PERMISSIONS_TABLE_NAME = 'rapid_users_permissions'


class DynamoDBAdapter:
    def __init__(self,
                 dynamodb_client=boto3.client('dynamodb', region_name=AWS_REGION),
                 permissions_table_name=PERMISSIONS_TABLE_NAME):
        self.dynamodb_client = dynamodb_client
        self.permissions_table_name = permissions_table_name

    def create_client_item(self, client_id: str, client_permissions: List[str]):
        db_permissions = self.get_db_permissions()
        permission_ids = self.get_validated_permission_ids(db_permissions, client_permissions)
        self.dynamodb_client.put_item(
            TableName=self.permissions_table_name,
            Item={
                "PK": {"S": "USR#CLIENT"},
                "SK": {"S": f"USR#${client_id}"},
                "Id": {"S": f"${client_id}"},
                "Type": {"S": "CLIENT"},
                "Permissions": {"SS": permission_ids}
            }
        )

    def get_validated_permission_ids(
            self, permissions_list: List[PermissionItem], user_permissions: List[str]) -> List[str]:
        valid_permission_ids = []
        for user_permission in user_permissions:
            permission_exists = False
            for permission_item in permissions_list:
                if user_permission == permission_item.permission:
                    permission_exists = True
                    valid_permission_ids.append(permission_item.id)
                    break
            if not permission_exists:
                raise UserError("One or more of the provided permissions do not exist")
        return valid_permission_ids

    def get_db_permissions(self) -> List[PermissionItem]:
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
        return [self._generate_permission_item(item) for item in table_items['Items']]

    def _generate_permission_item(self, item: dict) -> PermissionItem:
        permission = PermissionItem(
            perm_id=item['Id'][list(item['Id'])[0]],
            sensitivity=item['Sensitivity'][list(item['Sensitivity'])[0]] if 'Sensitivity' in item else None,
            perm_type=item['Type'][list(item['Type'])[0]]
        )
        return permission
