from api.adapter.cognito_adapter import CognitoAdapter
from api.adapter.dynamodb_adapter import DynamoDBAdapter
from api.domain.client import ClientResponse, ClientRequest


class ClientService:
    def __init__(
        self, cognito_adapter=CognitoAdapter(), dynamodb_adapter=DynamoDBAdapter()
    ):
        self.cognito_adapter = cognito_adapter
        self.dynamodb_adapter = dynamodb_adapter

    def create_client(self, client_request: ClientRequest) -> ClientResponse:
        self.dynamodb_adapter.validate_permissions(client_request.permissions)
        client_response = self._create_cognito_client(client_request)

        self._store_client_permissions(client_request, client_response)

        return client_response

    def _create_cognito_client(self, client_request: ClientRequest) -> ClientResponse:
        cognito_response = self.cognito_adapter.create_client_app(client_request)
        cognito_client_info = cognito_response["UserPoolClient"]
        client_response = ClientResponse(
            client_name=client_request.client_name,
            client_id=cognito_client_info["ClientId"],
            client_secret=cognito_client_info["ClientSecret"],
            permissions=client_request.permissions,
        )
        return client_response

    def _store_client_permissions(
        self, client_request: ClientRequest, client_response: ClientResponse
    ):
        try:
            self.dynamodb_adapter.store_client_permissions(
                client_response.client_id, client_request.permissions
            )
        except Exception as error:
            self.cognito_adapter.delete_client_app(client_response.client_id)
            raise error
