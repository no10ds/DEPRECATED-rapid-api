from typing import List

from api.common.config.auth import COGNITO_RESOURCE_SERVER_ID, SubjectType


class Token:
    def __init__(self, payload: dict):
        self.subject: str = self._extract_subject(payload)
        self.permissions: List[str] = self._extract_permissions(payload)

    def _extract_subject(self, payload: dict) -> str:
        try:
            subject = payload["sub"]
        except KeyError:
            raise ValueError("No Subject key")

        if not subject:
            raise ValueError("Invalid Subject field")

        return subject

    def _extract_permissions(self, payload: dict) -> List[str]:
        permission_scopes = payload.get("scope", None)

        if permission_scopes:
            self.token_type = SubjectType.CLIENT.value
            try:
                scopes = payload["scope"].split()
                return [
                    scope.split(COGNITO_RESOURCE_SERVER_ID + "/", 1)[1]
                    for scope in scopes
                ]
            except (AttributeError, IndexError):
                raise ValueError("Invalid scope field")

        return []
