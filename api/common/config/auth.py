import os
import urllib.parse
from typing import List

from api.common.config.aws import DOMAIN_NAME, RESOURCE_PREFIX, AWS_REGION
from api.common.utilities import BaseEnum


RAPID_ACCESS_TOKEN = "rat"  # nosec B105
COOKIE_MAX_AGE_IN_SECONDS = 3600
DEFAULT_SCOPE = ["READ_PUBLIC"]
COGNITO_ALLOWED_FLOWS = ["client_credentials"]
COGNITO_RESOURCE_SERVER_ID = "https://" + DOMAIN_NAME
COGNITO_USER_POOL_ID = os.environ["COGNITO_USER_POOL_ID"]
IDENTITY_PROVIDER_TOKEN_URL = (
    "https://"
    + RESOURCE_PREFIX
    + "-auth.auth."
    + AWS_REGION
    + ".amazoncognito.com/oauth2/token"
)
IDENTITY_PROVIDER_AUTHORIZATION_URL = (
    "https://"
    + RESOURCE_PREFIX
    + "-auth.auth."
    + AWS_REGION
    + ".amazoncognito.com/oauth2/authorize"
)
COGNITO_JWKS_URL = (
    "https://cognito-idp."
    + AWS_REGION
    + ".amazonaws.com/"
    + COGNITO_USER_POOL_ID
    + "/.well-known/jwks.json"
)
COGNITO_EXPLICIT_AUTH_FLOWS = [
    "ALLOW_REFRESH_TOKEN_AUTH",
    "ALLOW_CUSTOM_AUTH",
    "ALLOW_USER_SRP_AUTH",
]

COGNITO_USER_LOGIN_APP_CREDENTIALS_SECRETS_NAME = os.getenv(
    "COGNITO_USER_LOGIN_APP_CREDENTIALS_SECRETS_NAME", "rapid_cognito_user_secrets"
)
COGNITO_REDIRECT_URI = f"https://{DOMAIN_NAME}/oauth2/success"


def construct_user_auth_url(client_id: str):
    return f"{IDENTITY_PROVIDER_AUTHORIZATION_URL}?client_id={client_id}&response_type=code&redirect_uri={urllib.parse.quote_plus(COGNITO_REDIRECT_URI)}"


class Action(BaseEnum):
    READ = "READ"
    WRITE = "WRITE"
    DELETE = "DELETE"
    ADD_CLIENT = "ADD_CLIENT"
    ADD_SCHEMA = "ADD_SCHEMA"

    @staticmethod
    def standalone_actions() -> List:
        return [Action.ADD_CLIENT, Action.ADD_SCHEMA]


# Classifications
# TODO: Review the naming of these
class SensitivityLevel(BaseEnum):
    PUBLIC = "PUBLIC"
    PRIVATE = "PRIVATE"
    SENSITIVE = "SENSITIVE"
    PROTECTED = "PROTECTED"
