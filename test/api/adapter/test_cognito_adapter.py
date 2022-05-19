from unittest.mock import Mock

import pytest
from botocore.exceptions import ClientError

from api.adapter.cognito_adapter import CognitoAdapter
from api.common.config.auth import COGNITO_USER_POOL_ID
from api.common.config.aws import DOMAIN_NAME
from api.common.custom_exceptions import (
    AWSServiceError,
    UserError,
    UserGroupCreationError,
    UserGroupDeletionError,
)
from api.domain.client import ClientRequest


class TestCognitoAdapterClientMethods:
    cognito_boto_client = None

    def setup_method(self):
        self.cognito_boto_client = Mock()
        self.cognito_adapter = CognitoAdapter(self.cognito_boto_client)

    def test_create_client_app(self):
        client_request = ClientRequest(
            client_name="my_client", scopes=["WRITE_PUBLIC", "READ_PRIVATE"]
        )

        expected_response = {
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

        self.cognito_boto_client.create_user_pool_client.return_value = (
            expected_response
        )

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
            AllowedOAuthScopes=[
                f"https://{DOMAIN_NAME}/WRITE_PUBLIC",
                f"https://{DOMAIN_NAME}/READ_PRIVATE",
            ],
            AllowedOAuthFlowsUserPoolClient=True,
        )
        assert actual_response == expected_response

    def test_raises_error_when_scope_does_not_exist_in_aws(self):
        client_request = ClientRequest(client_name="my_client", scopes=["NOT_VALID"])

        self.cognito_boto_client.create_user_pool_client.side_effect = ClientError(
            error_response={"Error": {"Code": "ScopeDoesNotExistException"}},
            operation_name="CreateUserPoolClient",
        )

        with pytest.raises(
            UserError, match="One or more of the provided scopes do not exist"
        ):
            self.cognito_adapter.create_client_app(client_request)

    def test_raises_error_when_the_client_fails_to_create_in_aws(self):
        client_request = ClientRequest(client_name="my_client", scopes=["NOT_VALID"])

        self.cognito_boto_client.create_user_pool_client.side_effect = ClientError(
            error_response={"Error": {"Code": "InvalidParameterException"}},
            operation_name="CreateUserPoolClient",
        )

        with pytest.raises(
            AWSServiceError, match="The client 'my_client' could not be created"
        ):
            self.cognito_adapter.create_client_app(client_request)

    def test_create_user_group_with_domain_and_dataset(self):
        self.cognito_adapter.create_user_groups("my_domain", "my_dataset")

        self.cognito_boto_client.create_group.assert_called_once_with(
            GroupName="WRITE/my_domain/my_dataset", UserPoolId=COGNITO_USER_POOL_ID
        )

    def test_raises_error_when_create_user_group_fails(self):
        self.cognito_boto_client.create_group.side_effect = ClientError(
            error_response={"Error": {"Code": "SomeException"}},
            operation_name="CreateGroup",
        )

        with pytest.raises(
            UserGroupCreationError,
            match="User group creation failed for domain=\\[my_domain\\] dataset=\\[my_dataset\\]",
        ):
            self.cognito_adapter.create_user_groups("my_domain", "my_dataset")

        self.cognito_boto_client.create_group.assert_called_once_with(
            GroupName="WRITE/my_domain/my_dataset", UserPoolId=COGNITO_USER_POOL_ID
        )

    def test_delete_user_group_with_domain_and_dataset(self):
        self.cognito_adapter.delete_user_groups("my_domain", "my_dataset")

        self.cognito_boto_client.delete_group.assert_called_once_with(
            GroupName="WRITE/my_domain/my_dataset", UserPoolId=COGNITO_USER_POOL_ID
        )

    def test_raises_error_when_delete_user_group_fails(self):
        self.cognito_boto_client.delete_group.side_effect = ClientError(
            error_response={"Error": {"Code": "SomeException"}},
            operation_name="DeleteGroup",
        )

        with pytest.raises(
            UserGroupDeletionError,
            match="User group deletion failed for domain=\\[my_domain\\] dataset=\\[my_dataset\\]",
        ):
            self.cognito_adapter.delete_user_groups("my_domain", "my_dataset")

        self.cognito_boto_client.delete_group.assert_called_once_with(
            GroupName="WRITE/my_domain/my_dataset", UserPoolId=COGNITO_USER_POOL_ID
        )

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
        mock_response = {"ResponseMetadata": {"HTTPStatusCode": 401}}
        self.cognito_boto_client.describe_resource_server = Mock(
            return_value=mock_response
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

        mock_response = {"ResponseMetadata": {"HTTPStatusCode": 401}}
        self.cognito_boto_client.update_resource_server = Mock(
            return_value=mock_response
        )

        with pytest.raises(
            AWSServiceError,
            match='The scopes "scopes" could not be added, please contact system administrator',
        ):
            self.cognito_adapter.add_resource_server_scopes(
                "pool", "identifier", "scopes"
            )
