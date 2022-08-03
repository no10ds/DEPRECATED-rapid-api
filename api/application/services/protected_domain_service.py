import json
from typing import List

from api.adapter.cognito_adapter import CognitoAdapter
from api.adapter.dynamodb_adapter import DynamoDBAdapter
from api.adapter.ssm_adapter import SSMAdapter
from api.common.config.auth import (
    COGNITO_RESOURCE_SERVER_ID,
    COGNITO_USER_POOL_ID,
    SensitivityLevel,
    Action,
    PROTECTED_DOMAIN_PERMISSIONS_PARAMETER_NAME,
)
from api.domain.permission_item import PermissionItem


class ProtectedDomainService:
    def __init__(
        self,
        cognito_adapter=CognitoAdapter(),
        dynamodb_adapter=DynamoDBAdapter(),
        ssm_adapter=SSMAdapter(),
    ):
        self.cognito_adapter = cognito_adapter
        self.ssm_adapter = ssm_adapter
        self.dynamodb_table = dynamodb_adapter

    def create_protected_domain_permission(self, domain: str) -> None:
        domain = domain.upper().strip()

        generated_permissions = self._generate_protected_permission_items(domain)

        self.dynamodb_table.store_protected_permission(generated_permissions, domain)

        self._append_protected_permission_to_parameter(generated_permissions)

    def _append_protected_permission_to_parameter(
        self, permissions: List[PermissionItem]
    ) -> None:
        """
        This is to ensure that any user added permissions can be picked up by the terraform infrastructure
        """
        existing_permissions = json.loads(
            self.ssm_adapter.get_parameter(PROTECTED_DOMAIN_PERMISSIONS_PARAMETER_NAME)
        )
        existing_permissions.extend(
            [permission.to_dict() for permission in permissions]
        )
        self.ssm_adapter.put_parameter(
            PROTECTED_DOMAIN_PERMISSIONS_PARAMETER_NAME,
            json.dumps(existing_permissions),
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

    def _generate_protected_permission_items(self, domain) -> List[PermissionItem]:
        read_permission_item = PermissionItem(
            id=f"{Action.READ.value}_{SensitivityLevel.PROTECTED.value}_{domain}",
            type=Action.READ.value,
            sensitivity=SensitivityLevel.PROTECTED.value,
            domain=domain,
        )
        write_permission_item = PermissionItem(
            id=f"{Action.WRITE.value}_{SensitivityLevel.PROTECTED.value}_{domain}",
            type=Action.WRITE.value,
            sensitivity=SensitivityLevel.PROTECTED.value,
            domain=domain,
        )

        return [read_permission_item, write_permission_item]
