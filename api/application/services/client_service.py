from api.adapter.cognito_adapter import CognitoAdapter
from api.domain.client import ClientResponse, ClientRequest


class ClientService:
    def __init__(self, cognito_adapter=CognitoAdapter()):
        self.cognito_adapter = cognito_adapter

    def create_client(self, client_request: ClientRequest):
        cognito_response = self.cognito_adapter.create_client_app(client_request)
        cognito_client_info = cognito_response["UserPoolClient"]

        client_response = ClientResponse(
            client_name=client_request.client_name,
            client_id=cognito_client_info["ClientId"],
            client_secret=cognito_client_info["ClientSecret"],
            scopes=cognito_client_info["AllowedOAuthScopes"],
        )

        return client_response
