import os
import urllib.parse
from typing import List

from api.common.config.aws import DOMAIN_NAME, RESOURCE_PREFIX, AWS_REGION
from api.common.utilities import BaseEnum

RAPID_ACCESS_TOKEN = "rat"  # nosec B105
COOKIE_MAX_AGE_IN_SECONDS = 3600

DEFAULT_PERMISSION = ["READ_PUBLIC"]

COGNITO_ALLOWED_FLOWS = ["client_credentials"]
COGNITO_RESOURCE_SERVER_ID = f"https://{DOMAIN_NAME}"
COGNITO_USER_POOL_ID = os.environ["COGNITO_USER_POOL_ID"]

IDENTITY_PROVIDER_TOKEN_URL = (
    f"https://{RESOURCE_PREFIX}-auth.auth.{AWS_REGION}.amazoncognito.com/oauth2/token"
)
IDENTITY_PROVIDER_AUTHORIZATION_URL = f"https://{RESOURCE_PREFIX}-auth.auth.{AWS_REGION}.amazoncognito.com/oauth2/authorize"
COGNITO_JWKS_URL = f"https://cognito-idp.{AWS_REGION}.amazonaws.com/{COGNITO_USER_POOL_ID}/.well-known/jwks.json"

COGNITO_EXPLICIT_AUTH_FLOWS = [
    "ALLOW_REFRESH_TOKEN_AUTH",
    "ALLOW_CUSTOM_AUTH",
    "ALLOW_USER_SRP_AUTH",
]

COGNITO_USER_LOGIN_APP_CREDENTIALS_SECRETS_NAME = os.getenv(
    "COGNITO_USER_LOGIN_APP_CREDENTIALS_SECRETS_NAME", "rapid_user_secrets_cognito"
)

COGNITO_REDIRECT_URI = f"https://{DOMAIN_NAME}/oauth2/success"
PROTECTED_DOMAIN_SCOPES_PARAMETER_NAME = f"{RESOURCE_PREFIX}_protected_domain_scopes"


def construct_user_auth_url(client_id: str):
    redirect_uri = urllib.parse.quote_plus(COGNITO_REDIRECT_URI)
    return f"{IDENTITY_PROVIDER_AUTHORIZATION_URL}?client_id={client_id}&response_type=code&redirect_uri={redirect_uri}"


class Action(BaseEnum):
    READ = "READ"
    WRITE = "WRITE"
    USER_ADMIN = "USER_ADMIN"
    DATA_ADMIN = "DATA_ADMIN"

    @staticmethod
    def standalone_actions() -> List:
        return [Action.USER_ADMIN, Action.DATA_ADMIN]

    @staticmethod
    def standalone_action_values() -> List:
        return [Action.USER_ADMIN.value, Action.DATA_ADMIN.value]


# Classifications
class SensitivityLevel(BaseEnum):
    PUBLIC = "PUBLIC"
    PRIVATE = "PRIVATE"
    PROTECTED = "PROTECTED"


class SubjectType(BaseEnum):
    CLIENT = "CLIENT"
    USER = "USER"


class DatabaseItem(BaseEnum):
    SUBJECT = "SUBJECT"
    PERMISSION = "PERMISSION"
