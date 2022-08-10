from typing import List

from api.adapter.dynamodb_adapter import DynamoDBAdapter


class PermissionsService:
    def __init__(self, dynamodb_adapter=DynamoDBAdapter()):
        self.dynamodb_adapter = dynamodb_adapter

    def get_permissions(self):
        return self.dynamodb_adapter.get_all_permissions()

    def get_subject_permissions(self, subject_id: str) -> List[str]:
        return self.dynamodb_adapter.get_permissions_for_subject(subject_id)

    def get_ui_permissions(self) -> dict:
        pass
