from unittest.mock import Mock

import pytest
from botocore.exceptions import ClientError

from api.adapter.cognito_adapter import CognitoAdapter
from api.common.config.auth import COGNITO_USER_POOL_ID
from api.common.config.aws import DOMAIN_NAME
from api.common.custom_exceptions import AWSServiceError, UserError
from api.domain.client import ClientRequest, ClientResponse
from api.domain.user import UserResponse, UserRequest


class TestCognitoAdapterClientApps:
    cognito_boto_client = None
    cognito_adapter = None

    def setup_method(self):
        self.cognito_boto_client = Mock()
        self.cognito_adapter = CognitoAdapter(self.cognito_boto_client)

    def test_create_client_app(self):

        self.cognito_boto_client.list_user_pool_clients.return_value = {
            "UserPoolClients": []
        }

        client_request = ClientRequest(
            client_name="my_client", permissions=["WRITE_PUBLIC", "READ_PRIVATE"]
        )
        expected_response = ClientResponse(
            client_name="my_client",
            client_id="some_client",
            client_secret="some_secret",  # pragma: allowlist secret
            permissions=["WRITE_PUBLIC", "READ_PRIVATE"],
        )

        cognito_response = {
            "UserPoolClient": {
                "UserPoolId": COGNITO_USER_POOL_ID,
                "ClientName": "my_client",
                "ClientId": "some_client",
                "ClientSecret": "some_secret",  # pragma: allowlist secret
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
                "AllowedOAuthScopes": [f"https://{DOMAIN_NAME}/read"],
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

        self.cognito_boto_client.create_user_pool_client.return_value = cognito_response

        actual_response = self.cognito_adapter.create_client_app(client_request)

        self.cognito_boto_client.create_user_pool_client.assert_called_once_with(
            UserPoolId=COGNITO_USER_POOL_ID,
            ClientName="my_client",
            GenerateSecret=True,
            ExplicitAuthFlows=[
                "ALLOW_REFRESH_TOKEN_AUTH",
                "ALLOW_CUSTOM_AUTH",
                "ALLOW_USER_SRP_AUTH",
            ],
            AllowedOAuthFlows=["client_credentials"],
            AllowedOAuthScopes=[f"https://{DOMAIN_NAME}/CLIENT_APP"],
            AllowedOAuthFlowsUserPoolClient=True,
        )
        assert actual_response == expected_response

    def test_creates_client_app_with_default_allowed_oauth_scope(self):
        self.cognito_boto_client.list_user_pool_clients.return_value = {
            "UserPoolClients": []
        }

        client_request = ClientRequest(
            client_name="my_client", permissions=["WRITE_PUBLIC", "READ_PRIVATE"]
        )

        self.cognito_boto_client.create_user_pool_client.return_value = {
            "UserPoolClient": {
                "UserPoolId": COGNITO_USER_POOL_ID,
                "ClientName": "my_client",
                "ClientId": "some_client",
                "ClientSecret": "some_secret",  # pragma: allowlist secret
            }
        }

        self.cognito_adapter.create_client_app(client_request)

        self.cognito_boto_client.create_user_pool_client.assert_called_once_with(
            UserPoolId=COGNITO_USER_POOL_ID,
            ClientName="my_client",
            GenerateSecret=True,
            ExplicitAuthFlows=[
                "ALLOW_REFRESH_TOKEN_AUTH",
                "ALLOW_CUSTOM_AUTH",
                "ALLOW_USER_SRP_AUTH",
            ],
            AllowedOAuthFlows=["client_credentials"],
            AllowedOAuthScopes=[f"https://{DOMAIN_NAME}/CLIENT_APP"],
            AllowedOAuthFlowsUserPoolClient=True,
        )

    def test_delete_client_app(self):
        self.cognito_adapter.delete_client_app("client_id")
        self.cognito_boto_client.delete_user_pool_client.assert_called_once_with(
            UserPoolId=COGNITO_USER_POOL_ID, ClientId="client_id"
        )

    def test_raises_error_when_the_client_fails_to_create_in_aws(self):
        self.cognito_boto_client.list_user_pool_clients.return_value = {
            "UserPoolClients": []
        }

        client_request = ClientRequest(
            client_name="my_client", permissions=["NOT_VALID"]
        )

        self.cognito_boto_client.create_user_pool_client.side_effect = ClientError(
            error_response={"Error": {"Code": "InvalidParameterException"}},
            operation_name="CreateUserPoolClient",
        )

        with pytest.raises(
            AWSServiceError, match="The client 'my_client' could not be created"
        ):
            self.cognito_adapter.create_client_app(client_request)

    def test_throws_error_when_client_app_has_duplicate_name(self):
        client_request = ClientRequest(
            client_name="existing_name_2", permissions=["VALID"]
        )

        self.cognito_boto_client.list_user_pool_clients.return_value = {
            "UserPoolClients": [
                {"ClientName": "existing_name_1"},
                {"ClientName": "existing_name_2"},
            ]
        }

        with pytest.raises(
            UserError, match="Client name 'existing_name_2' already exists"
        ):
            self.cognito_adapter.create_client_app(client_request)


class TestCognitoAdapterUsers:
    cognito_boto_client = None
    cognito_adapter = None

    def setup_method(self):
        self.cognito_boto_client = Mock()
        self.cognito_adapter = CognitoAdapter(self.cognito_boto_client)

    def test_create_user(self):
        cognito_response = {
            "User": {
                "Username": "user-name",
                "Attributes": [
                    {"Name": "sub", "Value": "some-uu-id-b226-e5fd18c59b85"},
                    {"Name": "email_verified", "Value": "True"},
                    {"Name": "email", "Value": "user-name@example1.com"},
                ],
            },
            "ResponseMetadata": {
                "RequestId": "the-request-id-b368-fae5cebb746f",
                "HTTPStatusCode": 200,
            },
        }
        expected_response = UserResponse(
            username="user-name",
            email="user-name@example1.com",
            permissions=["WRITE_PUBLIC", "READ_PRIVATE"],
            user_id="some-uu-id-b226-e5fd18c59b85",
        )
        request = UserRequest(
            username="user-name",
            email="user-name@example1.com",
            permissions=["WRITE_PUBLIC", "READ_PRIVATE"],
        )
        self.cognito_boto_client.admin_create_user.return_value = cognito_response

        actual_response = self.cognito_adapter.create_user(request)
        self.cognito_boto_client.admin_create_user.assert_called_once_with(
            UserPoolId=COGNITO_USER_POOL_ID,
            Username="user-name",
            UserAttributes=[
                {"Name": "email", "Value": "user-name@example1.com"},
                {"Name": "email_verified", "Value": "True"},
            ],
            DesiredDeliveryMediums=[
                "EMAIL",
            ],
        )

        assert actual_response == expected_response

    def test_delete_user(self):
        self.cognito_adapter.delete_user("username")
        self.cognito_boto_client.admin_delete_user.assert_called_once_with(
            UserPoolId=COGNITO_USER_POOL_ID, Username="username"
        )

    def test_create_user_fails_in_aws(self):
        request = UserRequest(
            username="user-name",
            email="user-name@example1.com",
            permissions=["WRITE_PUBLIC", "READ_PRIVATE"],
        )

        self.cognito_boto_client.admin_create_user.side_effect = ClientError(
            error_response={"Error": {"Code": "InvalidParameterException"}},
            operation_name="AdminCreateUser",
        )

        with pytest.raises(
            AWSServiceError, match="The user 'user-name' could not be created"
        ):
            self.cognito_adapter.create_user(request)

    def test_create_user_fails_when_the_user_already_exist(self):
        request = UserRequest(
            username="user-name",
            email="user-name@example1.com",
            permissions=["WRITE_PUBLIC", "READ_PRIVATE"],
        )

        self.cognito_boto_client.admin_create_user.side_effect = ClientError(
            error_response={"Error": {"Code": "UsernameExistsException"}},
            operation_name="AdminCreateUser",
        )

        with pytest.raises(
            UserError,
            match="The user 'user-name' or email 'user-name@example1.com' already exist",
        ):
            self.cognito_adapter.create_user(request)


class TestCognitoResourceServer:
    cognito_boto_client = None
    cognito_adapter = None

    def setup_method(self):
        self.cognito_boto_client = Mock()
        self.cognito_adapter = CognitoAdapter(self.cognito_boto_client)

    def test_get_resource_server_success(self):
        expected_response = {
            "UserPoolId": "user_pool",
            "Identifier": "identifier",
            "Name": "name",
            "Scopes": [
                {"ScopeName": "scope_name", "ScopeDescription": "scope_description"}
            ],
        }

        mock_response = {
            "ResourceServer": expected_response,
            "ResponseMetadata": {"HTTPStatusCode": 200},
        }

        self.cognito_boto_client.describe_resource_server.return_value = mock_response

        actual_response = self.cognito_adapter.get_resource_server(
            "user_pool", "identifier"
        )

        self.cognito_boto_client.describe_resource_server.assert_called_once_with(
            UserPoolId="user_pool", Identifier="identifier"
        )

        assert actual_response == expected_response

    def test_get_resource_server_fails(self):
        self.cognito_boto_client.describe_resource_server = Mock(
            side_effect=ClientError(
                error_response={"Error": {"Code": "SomeException"}},
                operation_name="DescribeResourceServer",
            )
        )
        with pytest.raises(
            AWSServiceError,
            match="The resource server could not be found, please contact system administrator",
        ):
            self.cognito_adapter.get_resource_server("pool", "identifier")

    def test_add_resource_server_scopes_success(self):
        new_scope = [{"ScopeName": "new_scope", "ScopeDescription": "new_scope"}]

        self.cognito_boto_client.update_resource_server = Mock()

        mock_describe_response = {
            "UserPoolId": "user_pool",
            "Identifier": "identifier",
            "Name": "name",
            "Scopes": [
                {"ScopeName": "existing_scope", "ScopeDescription": "existing_scope"},
            ],
        }
        self.cognito_adapter.get_resource_server = Mock(
            return_value=mock_describe_response
        )
        mock_update_response = {
            "ResourceServer": {
                "UserPoolId": "user_pool",
                "Identifier": "identifier",
                "Name": "name",
                "Scopes": [
                    {
                        "ScopeName": "existing_scope",
                        "ScopeDescription": "existing_scope",
                    },
                    *new_scope,
                ],
            },
            "ResponseMetadata": {"HTTPStatusCode": 200},
        }
        self.cognito_boto_client.update_resource_server = Mock(
            return_value=mock_update_response
        )
        self.cognito_adapter.add_resource_server_scopes(
            "user_pool", "identifier", new_scope
        )

        self.cognito_boto_client.update_resource_server.assert_called_once_with(
            **mock_update_response["ResourceServer"]
        )

    def test_add_resource_server_scopes_fails(self):
        self.cognito_adapter.get_resource_server = Mock(return_value={"Scopes": []})

        self.cognito_boto_client.update_resource_server = Mock(
            side_effect=ClientError(
                error_response={"Error": {"Code": "SomeException"}},
                operation_name="UpdateResourceServer",
            )
        )

        with pytest.raises(
            AWSServiceError,
            match='The scopes "scopes" could not be added, please contact system administrator',
        ):
            self.cognito_adapter.add_resource_server_scopes(
                "pool", "identifier", "scopes"
            )
