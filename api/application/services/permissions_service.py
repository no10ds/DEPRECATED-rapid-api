from typing import Dict, List, Union

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
        return self.format_permissions_for_the_ui(all_permissions)

    def format_permissions_for_the_ui(
        self, permissions: List[PermissionItem]
    ) -> Dict[str, Union[str, Dict[str, Dict[str, str]]]]:
        result = {}

        for permission in permissions:
            # Protected Permission
            if permission.domain:
                result.setdefault(permission.type, {}).setdefault(
                    permission.layer, {}
                ).setdefault(permission.sensitivity, {})[
                    permission.domain
                ] = permission.id

            # Global Data Permission
            elif permission.sensitivity:
                result.setdefault(permission.type, {}).setdefault(permission.layer, {})[
                    permission.sensitivity
                ] = permission.id
            # Admin permission
            else:
                result[permission.type] = permission.id

        return result
