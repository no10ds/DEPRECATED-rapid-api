import boto3
from botocore.exceptions import ClientError
from typing import List

from api.common.config.auth import (
    COGNITO_RESOURCE_SERVER_ID,
    COGNITO_USER_POOL_ID,
    COGNITO_EXPLICIT_AUTH_FLOWS,
    COGNITO_ALLOWED_FLOWS,
)
from api.common.config.aws import AWS_REGION
from api.common.custom_exceptions import (
    AWSServiceError,
    UserError,
    UserGroupCreationError,
    UserGroupDeletionError,
)
from api.domain.client import ClientRequest


class CognitoAdapter:
    def __init__(
        self, cognito_client=boto3.client("cognito-idp", region_name=AWS_REGION)
    ):
        self.cognito_client = cognito_client

    def create_client_app(self, client_request: ClientRequest):
        try:
            cognito_scopes = self._build_cognito_scopes(client_request)

            cognito_response = self.cognito_client.create_user_pool_client(
                UserPoolId=COGNITO_USER_POOL_ID,
                ClientName=client_request.get_validated_client_name(),
                GenerateSecret=True,
                ExplicitAuthFlows=COGNITO_EXPLICIT_AUTH_FLOWS,
                AllowedOAuthFlows=COGNITO_ALLOWED_FLOWS,
                AllowedOAuthScopes=cognito_scopes,
                AllowedOAuthFlowsUserPoolClient=True,
            )

            return cognito_response
        except ClientError as error:
            self._handle_client_error(client_request, error)

    def create_user_groups(self, domain: str, dataset: str):
        try:
            self.cognito_client.create_group(
                GroupName=self._generate_user_group(domain, dataset),
                UserPoolId=COGNITO_USER_POOL_ID,
            )
        except ClientError:
            raise UserGroupCreationError(
                f"User group creation failed for domain=[{domain}] dataset=[{dataset}]"
            )

    def delete_user_groups(self, domain: str, dataset: str):
        try:
            self.cognito_client.delete_group(
                GroupName=self._generate_user_group(domain, dataset),
                UserPoolId=COGNITO_USER_POOL_ID,
            )
        except ClientError:
            raise UserGroupDeletionError(
                f"User group deletion failed for domain=[{domain}] dataset=[{dataset}]"
            )

    def _generate_user_group(self, domain: str, dataset: str) -> str:
        return f"WRITE/{domain}/{dataset}"

    def _build_cognito_scopes(self, client_request):
        return [
            f"{COGNITO_RESOURCE_SERVER_ID}/{scope}"
            for scope in client_request.get_permissions()
        ]

    def _handle_client_error(self, client_request, error):
        if error.response["Error"]["Code"] == "ScopeDoesNotExistException":
            raise UserError("One or more of the provided permissions does not exist")
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
