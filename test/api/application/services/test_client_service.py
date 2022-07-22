from unittest.mock import Mock

from api.application.services.client_service import ClientService
from api.common.config.aws import DOMAIN_NAME
from api.domain.client import ClientRequest, ClientResponse


class TestClientCreation:
    def setup_method(self):
        self.cognito_adapter = Mock()
        self.dynamo_adapter = Mock()
        self.client_service = ClientService(self.cognito_adapter, self.dynamo_adapter)

    def test_create_client(self):
        expected_response = ClientResponse(
            client_name="my_client",
            client_id="some-client-id",
            client_secret="some-client-secret",  # pragma: allowlist secret
            permissions=[
                f"https://{DOMAIN_NAME}/WRITE_PUBLIC",
                f"https://{DOMAIN_NAME}/READ_PRIVATE",
            ],
        )

        client_request = ClientRequest(
            client_name="my_client", permissions=["WRITE_PUBLIC", "READ_PRIVATE"]
        )

        self.cognito_adapter.create_client_app.return_value = self.cognito_response

        client_response = self.client_service.create_client(client_request)

        self.cognito_adapter.create_client_app.assert_called_once_with(client_request)

        self.dynamo_adapter.create_client_item.assert_called_once_with(
            expected_response.client_id, client_request.permissions)

        assert client_response == expected_response

    cognito_response = {
        "UserPoolClient": {
            "UserPoolId": "region-pool-id",
            "ClientName": "test3",
            "ClientId": "some-client-id",
            "ClientSecret": "some-client-secret",  # pragma: allowlist secret
            "LastModifiedDate": "datetime.datetime(2022, 2, 15, 16, 52, 17, 627000",
            "CreationDate": "datetime.datetime(2022, 2, 15, 16, 52, 17, 627000",
            "RefreshTokenValidity": 30,
            "TokenValidityUnits": {},
            "ExplicitAuthFlows": [
                "ALLOW_CUSTOM_AUTH",
                "ALLOW_USER_SRP_AUTH",
                "ALLOW_REFRESH_TOKEN_AUTH",
            ],
            "AllowedOAuthFlows": ["client_credentials"],
            "AllowedOAuthScopes": [
                f"https://{DOMAIN_NAME}/WRITE_PUBLIC",
                f"https://{DOMAIN_NAME}/READ_PRIVATE",
            ],
            "AllowedOAuthFlowsUserPoolClient": True,
            "EnableTokenRevocation": True,
        },
        "ResponseMetadata": {
            "RequestId": "7e5b5c39-8bf8-4082-a335-fe435a8014c6",
            "HTTPStatusCode": 200,
            "HTTPHeaders": {
                "date": "Tue, 15 Feb 2022 16:52:17 GMT",
                "content-type": "application/x-amz-json-1.1",
                "content-length": "568",
                "connection": "keep-alive",
                "x-amzn-requestid": "7e5b5c39-8bf8-4082-a335-fe435a8014c6",
            },
            "RetryAttempts": 0,
        },
    }
