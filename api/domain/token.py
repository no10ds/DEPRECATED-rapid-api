from typing import List

from api.common.config.auth import COGNITO_RESOURCE_SERVER_ID


class Token:
    def __init__(self, payload: dict):
        self.token_type = None
        self.subject: str = self._extract_subject(payload)
        self.permissions: List[str] = self._extract_permissions(payload)

    def is_client_token(self) -> bool:
        return self.token_type == "CLIENT"

    def is_user_token(self) -> bool:
        return self.token_type == "USER"

    def _extract_subject(self, payload: dict) -> str:
        try:
            subject = payload["sub"]
        except KeyError:
            raise ValueError("No Subject key")

        if not subject:
            raise ValueError("Invalid Subject field")

        return subject

    def _extract_permissions(self, payload: dict) -> List[str]:
        permission_groups = payload.get("cognito:groups", None)
        permission_scopes = payload.get("scope", None)

        if permission_groups:
            self.token_type = "USER"
            return permission_groups

        if permission_scopes:
            self.token_type = "CLIENT"
            try:
                scopes = payload["scope"].split()
                return [scope.split(COGNITO_RESOURCE_SERVER_ID + "/", 1)[1] for scope in scopes]
            except (AttributeError, IndexError):
                raise ValueError("Invalid scope field")

        return []
