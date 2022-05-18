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
        self.cognito_adapter.add_resource_server_scopes(
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
