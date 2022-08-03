import json
from typing import List
from unittest.mock import Mock

import pytest

from api.application.services.protected_domain_service import ProtectedDomainService
from api.common.config.auth import (
    COGNITO_USER_POOL_ID,
    COGNITO_RESOURCE_SERVER_ID,
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

    @pytest.mark.parametrize(
        "mock_scopes, expected_response",
        [
            (
                [
                    "READ_PROTECTED_DOMAIN",
                    "WRITE_PROTECTED_DOMAIN",
                    "WRITE_PROTECTED_OTHER",
                    "WRITE_ALL",
                    "DATA_ADMIN",
                    "READ_PUBLIC",
                ],
                ["other", "domain"],
            ),
            (["WRITE_ALL", "DATA_ADMIN", "READ_PUBLIC"], []),
        ],
    )
    def test_protected_domain_list(
        self, mock_scopes: List[str], expected_response: List[str]
    ):
        mock_response = {
            "Scopes": [
                {"ScopeName": scope, "ScopeDescription": "Description"}
                for scope in mock_scopes
            ]
        }

        self.cognito_adapter.get_resource_server = Mock(return_value=mock_response)

        res = self.protected_domain_service.list_domains()
        assert res == sorted(expected_response)
        self.cognito_adapter.get_resource_server.assert_called_once_with(
            COGNITO_USER_POOL_ID, COGNITO_RESOURCE_SERVER_ID
        )
