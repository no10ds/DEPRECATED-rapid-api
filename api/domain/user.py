import re
from typing import Optional, List

from pydantic import BaseModel

from api.common.config.auth import DEFAULT_PERMISSION
from api.common.custom_exceptions import UserError


class UserRequest(BaseModel):
    username: str
    email: str
    permissions: Optional[List[str]] = DEFAULT_PERMISSION

    def get_validated_username(self):
        """
        We restrict further beyond Cognito limits:
        https://docs.aws.amazon.com/cognito/latest/developerguide/limits.html
        """
        if self.username is not None and re.fullmatch(
            "[a-zA-Z][a-zA-Z0-9@._-]{2,127}", self.username
        ):
            return self.username
        raise UserError("Invalid user name provided")

    def get_permissions(self) -> List[str]:
        return self.permissions


class UserResponse(BaseModel):
    username: str
    email: str
    permissions: List[str]
    user_id: str
