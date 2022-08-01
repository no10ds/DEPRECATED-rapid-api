from typing import List

from api.common.config.auth import COGNITO_RESOURCE_SERVER_ID
from api.common.logger import AppLogger


class Token:
    def __init__(self, payload: dict):
        self.subject: str = self._extract_subject(payload)
        self.permissions: List[str] = self._extract_permissions(payload)

    def _extract_subject(self, payload: dict) -> str:
        try:
            subject = payload["sub"]
        except KeyError:
            AppLogger.info("No subject field defined in the payload.")
            raise ValueError("No Subject key")

        if not subject:
            AppLogger.info("No value for subject in the payload.")
            raise ValueError("Invalid Subject field")

        return subject

    def _extract_permissions(self, payload: dict) -> List[str]:
        permission_scopes = payload.get("scope", None)
        is_user = payload.get("username", None)

        if permission_scopes and not is_user:
            try:
                scopes = payload["scope"].split()
                return [
                    scope.split(COGNITO_RESOURCE_SERVER_ID + "/", 1)[1]
                    for scope in scopes
                ]
            except (AttributeError, IndexError):
                AppLogger.info("Invalid scope field")
                raise ValueError("Invalid scope field")

        return []
