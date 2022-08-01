from typing import Optional, List

from fastapi import Depends, HTTPException
from fastapi.openapi.models import OAuthFlows as OAuthFlowsModel
from fastapi.security import OAuth2
from fastapi.security import SecurityScopes
from fastapi.security.utils import get_authorization_scheme_param
from jwt import InvalidTokenError
from starlette.requests import Request
from starlette.status import HTTP_401_UNAUTHORIZED

from api.adapter.dynamodb_adapter import DynamoDBAdapter
from api.adapter.s3_adapter import S3Adapter
from api.application.services.authorisation.acceptable_permissions import (
    generate_acceptable_scopes,
)
from api.application.services.authorisation.token_utils import parse_token
from api.common.config.auth import (
    IDENTITY_PROVIDER_TOKEN_URL,
    RAPID_ACCESS_TOKEN,
)
from api.common.custom_exceptions import (
    SchemaNotFoundError,
    AuthorisationError,
    UserCredentialsUnavailableError,
    SubjectNotFoundError,
    ClientCredentialsUnavailableError,
)
from api.common.logger import AppLogger
from api.domain.token import Token


class OAuth2ClientCredentials(OAuth2):
    def __init__(
        self,
        token_url: str,
        scheme_name: str = None,
        allowed_scopes: dict = None,
        auto_error: bool = True,
    ):
        if not allowed_scopes:
            allowed_scopes = {}
        flows = OAuthFlowsModel(
            clientCredentials={"tokenUrl": token_url, "scopes": allowed_scopes}
        )
        super().__init__(flows=flows, scheme_name=scheme_name, auto_error=auto_error)

    async def __call__(self, request: Request) -> Optional[str]:
        authorization: str = request.headers.get("Authorization")
        scheme, jwt_token = get_authorization_scheme_param(authorization)
        if not authorization or scheme.lower() != "bearer":
            return None
        return jwt_token


class OAuth2UserCredentials:
    async def __call__(self, request: Request) -> Optional[str]:
        return request.cookies.get(RAPID_ACCESS_TOKEN, None)


oauth2_scheme = OAuth2ClientCredentials(token_url=IDENTITY_PROVIDER_TOKEN_URL)
oauth2_user_scheme = OAuth2UserCredentials()
s3_adapter = S3Adapter()
db_adapter = DynamoDBAdapter()


def is_browser_request(request: Request) -> bool:
    accept_type = request.headers.get("Accept")
    return accept_type.startswith("text/html")


def user_logged_in(request: Request) -> bool:
    token = request.cookies.get(RAPID_ACCESS_TOKEN, None)
    return token is not None


def secure_endpoint(
    security_scopes: SecurityScopes,
    browser_request: bool = Depends(is_browser_request),
    client_token: Optional[str] = Depends(oauth2_scheme),
    user_token: Optional[str] = Depends(oauth2_user_scheme),
):
    secure_dataset_endpoint(security_scopes, browser_request, client_token, user_token)


def secure_dataset_endpoint(
    security_scopes: SecurityScopes,
    browser_request: bool = Depends(is_browser_request),
    client_token: Optional[str] = Depends(oauth2_scheme),
    user_token: Optional[str] = Depends(oauth2_user_scheme),
    domain: Optional[str] = None,
    dataset: Optional[str] = None,
):
    check_credentials_availability(browser_request, user_token, client_token)

    try:
        token = user_token if user_token else client_token
        token = parse_token(token)
        check_permissions(token, security_scopes.scopes, domain, dataset)
    except InvalidTokenError as error:
        raise HTTPException(status_code=HTTP_401_UNAUTHORIZED, detail=str(error))


def check_credentials_availability(
    browser_request: bool, user_token: Optional[str], client_token: Optional[str]
) -> None:
    if not have_credentials(browser_request, client_token, user_token):
        if browser_request:
            AppLogger.info("Cannot retrieve user credentials if no user token provided")
            raise UserCredentialsUnavailableError()
        else:
            AppLogger.info(
                "Cannot retrieve client credentials if no client token provided"
            )
            raise ClientCredentialsUnavailableError(
                message="You are not authorised to perform this action", status_code=403
            )


def have_credentials(browser_request: bool, client_token: str, user_token: str) -> bool:
    return bool((browser_request and user_token) or client_token)


def check_permissions(
    token: Token,
    endpoint_scopes: List[str],
    domain: Optional[str],
    dataset: Optional[str],
):
    try:
        subject_permissions = retrieve_permissions(token)
        match_permissions(subject_permissions, endpoint_scopes, domain, dataset)
    except SchemaNotFoundError:
        AppLogger.info(f"Dataset [{dataset}] in domain [{domain}] does not exist")
        raise HTTPException(
            status_code=400,
            detail=f"Dataset [{dataset}] in domain [{domain}] does not exist",
        )


def retrieve_permissions(token: Token) -> List[str]:
    try:
        database_permissions = db_adapter.get_permissions_for_subject(token.subject)
        return database_permissions if database_permissions else token.permissions
    except SubjectNotFoundError:
        return token.permissions


def match_permissions(
    permissions: list, endpoint_scopes: list, domain: str = None, dataset: str = None
):
    sensitivity = s3_adapter.get_dataset_sensitivity(domain, dataset)
    acceptable_scopes = generate_acceptable_scopes(endpoint_scopes, sensitivity, domain)
    if not acceptable_scopes.satisfied_by(permissions):
        AppLogger.info(
            f"Users list of permissions: {permissions} do not match endpoint permissions: {acceptable_scopes}."
        )
        raise AuthorisationError("Not enough permissions to access endpoint")
