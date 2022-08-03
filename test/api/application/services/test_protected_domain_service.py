import json
from unittest.mock import Mock

from api.application.services.protected_domain_service import ProtectedDomainService
from api.common.config.auth import (
    PROTECTED_DOMAIN_PERMISSIONS_PARAMETER_NAME,
)
from api.domain.permission_item import PermissionItem


class TestProtectedDomainService:
    def setup_method(self):
        self.cognito_adapter = Mock()
        self.dynamodb_adapter = Mock()
        self.ssm_adapter = Mock()
        self.protected_domain_service = ProtectedDomainService(
            self.cognito_adapter, self.dynamodb_adapter, self.ssm_adapter
        )

    def test_create_protected_domain_permission(self):
        generated_permissions = [
            PermissionItem(
                id="READ_PROTECTED_DOMAIN",
                type="READ",
                sensitivity="PROTECTED",
                domain="DOMAIN",
            ),
            PermissionItem(
                id="WRITE_PROTECTED_DOMAIN",
                type="WRITE",
                sensitivity="PROTECTED",
                domain="DOMAIN",
            ),
        ]

        existing_parameters = json.dumps(
            [
                {
                    "PermissionName": "READ_PROTECTED_BUS",
                    "Type": "READ",
                    "Sensitivity": "PROTECTED",
                    "Domain": "BUS",
                },
                {
                    "PermissionName": "WRITE_PROTECTED_BUS",
                    "Type": "WRITE",
                    "Sensitivity": "PROTECTED",
                    "Domain": "BUS",
                },
            ]
        )

        existing_and_new_permissions = [
            {
                "PermissionName": "READ_PROTECTED_BUS",
                "Type": "READ",
                "Sensitivity": "PROTECTED",
                "Domain": "BUS",
            },
            {
                "PermissionName": "WRITE_PROTECTED_BUS",
                "Type": "WRITE",
                "Sensitivity": "PROTECTED",
                "Domain": "BUS",
            },
            {
                "PermissionName": "READ_PROTECTED_DOMAIN",
                "Type": "READ",
                "Sensitivity": "PROTECTED",
                "Domain": "DOMAIN",
            },
            {
                "PermissionName": "WRITE_PROTECTED_DOMAIN",
                "Type": "WRITE",
                "Sensitivity": "PROTECTED",
                "Domain": "DOMAIN",
            },
        ]
        self.ssm_adapter.get_parameter.return_value = existing_parameters

        self.protected_domain_service.create_protected_domain_permission("domain")

        self.dynamodb_adapter.store_protected_permission.assert_called_once_with(
            generated_permissions, "DOMAIN"
        )

        self.ssm_adapter.get_parameter.assert_called_once_with(
            PROTECTED_DOMAIN_PERMISSIONS_PARAMETER_NAME
        )
        self.ssm_adapter.put_parameter.assert_called_once_with(
            PROTECTED_DOMAIN_PERMISSIONS_PARAMETER_NAME,
            json.dumps(existing_and_new_permissions),
        )

    def test_protected_domain_list(self):
        expected_response = ["other", "domain"]
        domain_permissions = [
            PermissionItem(
                id="READ_PROTECTED_OTHER",
                type="READ",
                sensitivity="PROTECTED",
                domain="OTHER",
            ),
            PermissionItem(
                id="WRITE_PROTECTED_OTHER",
                type="WRITE",
                sensitivity="PROTECTED",
                domain="OTHER",
            ),
            PermissionItem(
                id="READ_PROTECTED_DOMAIN",
                type="READ",
                sensitivity="PROTECTED",
                domain="DOMAIN",
            ),
            PermissionItem(
                id="WRITE_PROTECTED_DOMAIN",
                type="WRITE",
                sensitivity="PROTECTED",
                domain="DOMAIN",
            ),
        ]

        self.dynamodb_adapter.get_all_protected_permissions.return_value = (
            domain_permissions
        )

        domains = self.protected_domain_service.list_domains()
        assert domains == set(sorted(expected_response))
        self.dynamodb_adapter.get_all_protected_permissions.assert_called_once()
