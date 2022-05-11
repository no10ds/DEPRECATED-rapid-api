import boto3
from botocore.exceptions import ClientError

from api.common.config.auth import COGNITO_RESOURCE_SERVER_ID, COGNITO_USER_POOL_ID, COGNITO_EXPLICIT_AUTH_FLOWS, \
    COGNITO_ALLOWED_FLOWS
from api.common.config.aws import AWS_REGION
from api.common.custom_exceptions import AWSServiceError, UserError, UserGroupCreationError, UserGroupDeletionError
from api.domain.client import ClientRequest


class CognitoAdapter:

    def __init__(self,
                 cognito_client=boto3.client('cognito-idp', region_name=AWS_REGION)):
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
                AllowedOAuthFlowsUserPoolClient=True)

            return cognito_response
        except ClientError as error:
            self._handle_client_error(client_request, error)

    def create_user_groups(self, domain: str, dataset: str):
        try:
            self.cognito_client.create_group(
                GroupName=self._generate_user_group(domain, dataset),
                UserPoolId=COGNITO_USER_POOL_ID
            )
        except ClientError:
            raise UserGroupCreationError(
                f"User group creation failed for domain=[{domain}] dataset=[{dataset}]")

    def delete_user_groups(self, domain: str, dataset: str):
        try:
            self.cognito_client.delete_group(
                GroupName=self._generate_user_group(domain, dataset),
                UserPoolId=COGNITO_USER_POOL_ID)
        except ClientError:
            raise UserGroupDeletionError(
                f"User group deletion failed for domain=[{domain}] dataset=[{dataset}]")

    def _generate_user_group(self, domain: str, dataset: str) -> str:
        return f'WRITE/{domain}/{dataset}'

    def _build_cognito_scopes(self, client_request):
        cognito_scopes = []
        for scope in client_request.get_scopes():
            cognito_scopes.append(f"{COGNITO_RESOURCE_SERVER_ID}/{scope}")
        return cognito_scopes

    def _handle_client_error(self, client_request, error):
        if error.response["Error"]["Code"] == "ScopeDoesNotExistException":
            raise UserError("One or more of the provided scopes do not exist")
        raise AWSServiceError(f"The client '{client_request.client_name}' could not be created")
