from typing import List


from api.adapter.cognito_adapter import CognitoAdapter
from api.common.config.auth import (
    COGNITO_RESOURCE_SERVER_ID,
    COGNITO_USER_POOL_ID,
    Action,
    SensitivityLevel,
)


class ProtectedDomainService:
    def __init__(self, cognito_adapter=CognitoAdapter()):
        self.cognito_adapter = cognito_adapter

    def create_scopes(self, domain: str):
        domain = domain.upper().strip()
        self.cognito_adapter.update_resource_server_scopes(
            COGNITO_USER_POOL_ID,
            COGNITO_RESOURCE_SERVER_ID,
            [
                {
                    "ScopeName": f"{Action.READ.value}_{SensitivityLevel.PROTECTED.value}_{domain}",
                    "ScopeDescription": f"Read from the protected domain of {domain}",
                },
                {
                    "ScopeName": f"{Action.WRITE.value}_{SensitivityLevel.PROTECTED.value}_{domain}",
                    "ScopeDescription": f"Write to the protected domain of {domain}",
                },
            ],
        )

    def list_domains(self) -> List[str]:
        scopes = self.cognito_adapter.get_resource_server(
            COGNITO_USER_POOL_ID, COGNITO_RESOURCE_SERVER_ID
        )["Scopes"]

        protected_scopes = [
            scope["ScopeName"]
            for scope in scopes
            if SensitivityLevel.PROTECTED.value in scope["ScopeName"]
        ]

        protected_domains = set(
            scope.split(SensitivityLevel.PROTECTED.value)[1].strip("_").lower()
            for scope in protected_scopes
        )

        return sorted(list(protected_domains))
