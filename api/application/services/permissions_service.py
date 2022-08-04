from api.adapter.dynamodb_adapter import DynamoDBAdapter


class PermissionsService:
    def __init__(self, dynamodb_adapter=DynamoDBAdapter()):
        self.dynamodb_adapter = dynamodb_adapter

    def get_permissions(self):
        return self.dynamodb_adapter.get_all_permissions()
