from typing import List, Set

from api.adapter.aws_resource_adapter import AWSResourceAdapter
from api.adapter.cognito_adapter import CognitoAdapter
from api.adapter.dynamodb_adapter import DynamoDBAdapter
from api.application.services.schema_validation import valid_domain_name
from api.common.config.auth import (
    SensitivityLevel,
    Action,
)
from api.common.custom_exceptions import ConflictError, UserError
from api.common.logger import AppLogger
from api.domain.permission_item import PermissionItem


class ProtectedDomainService:
    def __init__(
        self,
        cognito_adapter=CognitoAdapter(),
        dynamodb_adapter=DynamoDBAdapter(),
        resource_adapter=AWSResourceAdapter(),
    ):
        self.cognito_adapter = cognito_adapter
        self.dynamodb_adapter = dynamodb_adapter
        self.resource_adapter = resource_adapter

    def create_protected_domain_permission(self, domain: str) -> None:
        AppLogger.info(f"Creating protected domain permission {domain}")
        domain = domain.upper().strip()

        if not valid_domain_name(domain):
            raise UserError(
                f"The value set for domain [{domain}] can only contain alphanumeric and underscore `_` characters and must start with an alphabetic character"
            )

        self._verify_protected_domain_does_not_exist(domain)

        generated_permissions = self._generate_protected_permission_items(domain)

        self.dynamodb_adapter.store_protected_permissions(generated_permissions, domain)

    def list_protected_domains(self) -> Set[str]:
        return self._list_protected_permission_domains()

    def _list_protected_permission_domains(self):
        permission_items = self.dynamodb_adapter.get_all_protected_permissions()
        return set([item.domain.lower() for item in permission_items])

    def _verify_protected_domain_does_not_exist(self, domain):
        if domain.lower() in self._list_protected_permission_domains():
            AppLogger.info(f"The protected domain, [{domain}] already exists")
            raise ConflictError(f"The protected domain, [{domain}] already exists")

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
