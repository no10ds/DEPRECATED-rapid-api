from api.adapter.cognito_adapter import CognitoAdapter
from api.adapter.dynamodb_adapter import DynamoDBAdapter
from api.domain.client import ClientResponse, ClientRequest


class ClientService:
    def __init__(self, cognito_adapter=CognitoAdapter(), dynamodb_adapter=DynamoDBAdapter()):
        self.cognito_adapter = cognito_adapter
        self.dynamodb_adapter = dynamodb_adapter

    def create_client(self, client_request: ClientRequest):
        cognito_response = self.cognito_adapter.create_client_app(client_request)
        cognito_client_info = cognito_response["UserPoolClient"]

        client_response = ClientResponse(
            client_name=client_request.client_name,
            client_id=cognito_client_info["ClientId"],
            client_secret=cognito_client_info["ClientSecret"],
            permissions=cognito_client_info["AllowedOAuthScopes"],
        )

        self.dynamodb_adapter.create_client_item(client_response.client_id, client_request.permissions)

        return client_response
