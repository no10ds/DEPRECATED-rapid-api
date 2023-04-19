from typing import List

from api.adapter.dynamodb_adapter import DynamoDBAdapter
from api.domain.permission_item import PermissionItem


class PermissionsService:
    def __init__(self, dynamodb_adapter=DynamoDBAdapter()):
        self.dynamodb_adapter = dynamodb_adapter

    def get_permissions(self) -> List[str]:
        return [
            permission.id for permission in self.dynamodb_adapter.get_all_permissions()
        ]

    def get_subject_permission_keys(self, subject_id: str) -> List[str]:
        return self.dynamodb_adapter.get_permission_keys_for_subject(subject_id)

    def get_subject_permissions(self, subject_id: str) -> List[PermissionItem]:
        permission_keys = self.get_subject_permission_keys(subject_id)
        all_permissions = self.dynamodb_adapter.get_all_permissions()
        return [
            permission.to_dict()
            for permission in all_permissions
            if permission.id in permission_keys
        ]

    def get_all_permissions_ui(self) -> List[dict]:
        all_permissions = self.dynamodb_adapter.get_all_permissions()
        return [permission.to_dict() for permission in all_permissions]
