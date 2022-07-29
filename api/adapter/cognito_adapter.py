import boto3
from botocore.exceptions import ClientError
from typing import List, Dict, Any

from api.common.config.auth import (
    COGNITO_RESOURCE_SERVER_ID,
    COGNITO_USER_POOL_ID,
    COGNITO_EXPLICIT_AUTH_FLOWS,
    COGNITO_ALLOWED_FLOWS,
)
from api.common.config.aws import AWS_REGION
from api.common.custom_exceptions import (
    AWSServiceError,
    UserGroupCreationError,
    UserGroupDeletionError,
)
from api.domain.client import ClientRequest, ClientResponse
from api.domain.user import UserRequest, UserResponse


class CognitoAdapter:
    def __init__(
        self, cognito_client=boto3.client("cognito-idp", region_name=AWS_REGION)
    ):
        self.cognito_client = cognito_client

    def create_client_app(self, client_request: ClientRequest) -> ClientResponse:
        try:
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

    def create_user(self, user_request: UserRequest) -> UserResponse:
        cognito_response = self.cognito_client.admin_create_user(
            UserPoolId=COGNITO_USER_POOL_ID,
            Username=user_request.get_validated_username(),
            UserAttributes=[
                {"Name": "email", "Value": user_request.email},
                {"Name": "email_verified", "Value": "True"},
            ],
            DesiredDeliveryMediums=[
                "EMAIL",
            ],
        )
        return self._create_user_response(cognito_response, user_request.permissions)

    def create_user_groups(self, domain: str, dataset: str) -> None:
        try:
            self.cognito_client.create_group(
                GroupName=self._generate_user_group(domain, dataset),
                UserPoolId=COGNITO_USER_POOL_ID,
            )
        except ClientError:
            raise UserGroupCreationError(
                f"User group creation failed for domain=[{domain}] dataset=[{dataset}]"
            )

    def delete_user_groups(self, domain: str, dataset: str) -> None:
        try:
            self.cognito_client.delete_group(
                GroupName=self._generate_user_group(domain, dataset),
                UserPoolId=COGNITO_USER_POOL_ID,
            )
        except ClientError:
            raise UserGroupDeletionError(
                f"User group deletion failed for domain=[{domain}] dataset=[{dataset}]"
            )

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
            permissions=client_request.permissions,
        )
        return client_response

    @staticmethod
    def _get_attribute_value(attribute_name: str, attributes: List[dict]):
        response_list = [attr for attr in attributes if attr["Name"] == attribute_name]
        return response_list[0]["Value"]

    def _generate_user_group(self, domain: str, dataset: str) -> str:
        return f"WRITE/{domain}/{dataset}"

    def _build_default_scopes(self):
        return [f"{COGNITO_RESOURCE_SERVER_ID}/CLIENT_APP"]

    def _handle_client_error(self, client_request, error):
        raise AWSServiceError(
            f"The client '{client_request.client_name}' could not be created"
        )

    def get_resource_server(self, user_pool_id: str, identifier: str):
        try:
            response = self.cognito_client.describe_resource_server(
                UserPoolId=user_pool_id, Identifier=identifier
            )
        except ClientError:
            raise AWSServiceError(
                "The resource server could not be found, please contact system administrator"
            )

        return response["ResourceServer"]

    def add_resource_server_scopes(
        self, user_pool_id: str, identifier: str, additional_scopes: List[dict]
    ):
        resource_server = self.get_resource_server(user_pool_id, identifier)
        resource_server["Scopes"].extend(additional_scopes)
        try:
            self.cognito_client.update_resource_server(**resource_server)
        except ClientError:
            raise AWSServiceError(
                f'The scopes "{additional_scopes}" could not be added, please contact system administrator'
            )

    def delete_client_app(self, client_id):
        self.cognito_client.delete_user_pool_client(
            UserPoolId=COGNITO_USER_POOL_ID, ClientId=client_id
        )
