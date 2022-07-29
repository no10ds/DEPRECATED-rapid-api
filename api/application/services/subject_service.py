from api.adapter.cognito_adapter import CognitoAdapter
from api.adapter.dynamodb_adapter import DynamoDBAdapter
from api.common.config.auth import SubjectType
from api.domain.client import ClientResponse, ClientRequest
from api.domain.user import UserRequest, UserResponse


class SubjectService:
    def __init__(
        self, cognito_adapter=CognitoAdapter(), dynamodb_adapter=DynamoDBAdapter()
    ):
        self.cognito_adapter = cognito_adapter
        self.dynamodb_adapter = dynamodb_adapter

    def create_client(self, client_request: ClientRequest) -> ClientResponse:
        self.dynamodb_adapter.validate_permissions(client_request.permissions)
        client_response = self.cognito_adapter.create_client_app(client_request)
        self._store_client_permissions(client_request, client_response)

        return client_response

    def create_user(self, user_request: UserRequest) -> UserResponse:
        self.dynamodb_adapter.validate_permissions(user_request.permissions)
        user_response = self.cognito_adapter.create_user(user_request)
        self.dynamodb_adapter.store_subject_permissions(
            SubjectType.USER.value, user_response.user_id, user_request.permissions
        )

        return user_response

    def _store_client_permissions(
        self, client_request: ClientRequest, client_response: ClientResponse
    ):
        try:
            self.dynamodb_adapter.store_subject_permissions(
                SubjectType.CLIENT.value,
                client_response.client_id,
                client_request.permissions,
            )
        except Exception as error:
            self.cognito_adapter.delete_client_app(client_response.client_id)
            raise error
