from typing import List

import boto3
from botocore.exceptions import ClientError

from api.common.config.auth import (
    COGNITO_RESOURCE_SERVER_ID,
    COGNITO_USER_POOL_ID,
    COGNITO_EXPLICIT_AUTH_FLOWS,
    COGNITO_ALLOWED_FLOWS,
    SensitivityLevel,
)
from api.common.config.aws import AWS_REGION
from api.common.custom_exceptions import AWSServiceError, UserError
from api.common.logger import AppLogger
from api.domain.client import ClientRequest, ClientResponse
from api.domain.user import UserRequest, UserResponse


class CognitoAdapter:
    def __init__(
        self, cognito_client=boto3.client("cognito-idp", region_name=AWS_REGION)
    ):
        self.cognito_client = cognito_client
        self.placeholder_client_name = "string"

    def create_client_app(self, client_request: ClientRequest) -> ClientResponse:

        try:
            AppLogger.info(
                f"Creating Client App with name: {client_request.client_name}"
            )
            self._validate_client_name(client_request.client_name)

            cognito_scopes = self._build_default_scopes()

            cognito_response = self.cognito_client.create_user_pool_client(
                UserPoolId=COGNITO_USER_POOL_ID,
                ClientName=client_request.get_validated_client_name(),
                GenerateSecret=True,
                ExplicitAuthFlows=COGNITO_EXPLICIT_AUTH_FLOWS,
                AllowedOAuthFlows=COGNITO_ALLOWED_FLOWS,
                AllowedOAuthScopes=cognito_scopes,
                AllowedOAuthFlowsUserPoolClient=True,
            )

            return self._create_client_response(
                client_request, cognito_response["UserPoolClient"]
            )
        except ClientError as error:
            self._handle_client_error(client_request, error)

    def _validate_client_name(self, client_name: str) -> None:
        if client_name == self.placeholder_client_name:
            raise UserError("You must specify a valid client name")

        existing_clients = self.cognito_client.list_user_pool_clients(
            UserPoolId=COGNITO_USER_POOL_ID
        )
        existing_client_names = [
            client.get("ClientName")
            for client in existing_clients.get("UserPoolClients", [])
        ]
        if client_name in existing_client_names:
            raise UserError(f"Client name '{client_name}' already exists")

    def create_user(self, user_request: UserRequest) -> UserResponse:
        try:
            cognito_response = self.cognito_client.admin_create_user(
                UserPoolId=COGNITO_USER_POOL_ID,
                Username=user_request.get_validated_username(),
                UserAttributes=[
                    {"Name": "email", "Value": user_request.get_validated_email()},
                    {"Name": "email_verified", "Value": "True"},
                ],
                DesiredDeliveryMediums=[
                    "EMAIL",
                ],
            )
            return self._create_user_response(
                cognito_response, user_request.get_permissions()
            )
        except ClientError as error:
            self._handle_user_error(user_request, error)

    def _create_user_response(
        self, cognito_response: dict, permissions: List[str]
    ) -> UserResponse:
        cognito_user = cognito_response["User"]
        return UserResponse(
            username=cognito_user["Username"],
            email=self._get_attribute_value("email", cognito_user["Attributes"]),
            permissions=permissions,
            user_id=self._get_attribute_value("sub", cognito_user["Attributes"]),
        )

    def _create_client_response(
        self, client_request: ClientRequest, cognito_client_info: dict
    ) -> ClientResponse:
        client_response = ClientResponse(
            client_name=client_request.client_name,
            client_id=cognito_client_info["ClientId"],
            client_secret=cognito_client_info["ClientSecret"],
            permissions=client_request.get_permissions(),
        )
        return client_response

    @staticmethod
    def _get_attribute_value(attribute_name: str, attributes: List[dict]):
        response_list = [attr for attr in attributes if attr["Name"] == attribute_name]
        return response_list[0]["Value"]

    def _build_default_scopes(self):
        return [f"{COGNITO_RESOURCE_SERVER_ID}/CLIENT_APP"]

    def _handle_client_error(self, client_request: ClientRequest, error):
        AppLogger.info(
            f"The client '{client_request.client_name}' could not be created with error: {error}"
        )
        raise AWSServiceError(
            f"The client '{client_request.client_name}' could not be created"
        )

    def _handle_user_error(self, user_request: UserRequest, error: ClientError):
        AppLogger.info(
            f"The user '{user_request.username}' could not be created with error: {error}"
        )
        if error.response["Error"]["Code"] == "UsernameExistsException":
            raise UserError(
                f"The user '{user_request.username}' or email '{user_request.email}' already exist"
            )
        raise AWSServiceError(
            f"The user '{user_request.username}' could not be created"
        )

    def get_protected_scopes(self):
        try:
            response = self.cognito_client.describe_resource_server(
                UserPoolId=COGNITO_USER_POOL_ID, Identifier=COGNITO_RESOURCE_SERVER_ID
            )["ResourceServer"]["Scopes"]
            return [
                item["ScopeName"]
                for item in response
                if SensitivityLevel.PROTECTED.value in item["ScopeName"]
            ]
        except ClientError as error:
            AppLogger.error(f"Unable to retrieve resource server information {error})")
            raise AWSServiceError(
                "Internal server error, please contact system administrator"
            )

    def delete_client_app(self, client_id: str):
        AppLogger.info(f"Deleting client {client_id}")
        self.cognito_client.delete_user_pool_client(
            UserPoolId=COGNITO_USER_POOL_ID, ClientId=client_id
        )

    def delete_user(self, username: str):
        AppLogger.info(f"Deleting user {username}")
        self.cognito_client.admin_delete_user(
            UserPoolId=COGNITO_USER_POOL_ID,
            Username=username,
        )
