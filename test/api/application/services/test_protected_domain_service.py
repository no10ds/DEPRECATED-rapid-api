import pytest
from unittest.mock import Mock
from typing import List

from api.application.services.protected_domain_service import ProtectedDomainService
from api.common.config.auth import (
    COGNITO_USER_POOL_ID,
    COGNITO_RESOURCE_SERVER_ID,
    PROTECTED_DOMAIN_SCOPES_PARAMETER_NAME,
)


class TestProtectedDomainService:
    def setup_method(self):
        self.cognito_adapter = Mock()
        self.ssm_adapter = Mock()
        self.protected_domain_service = ProtectedDomainService(
            self.cognito_adapter, self.ssm_adapter
        )

    def test_create_scopes(self):
        expected_scopes = [
            {
                "ScopeName": "READ_PROTECTED_DOMAIN",
                "ScopeDescription": "Read from the protected domain of DOMAIN",
            },
            {
                "ScopeName": "WRITE_PROTECTED_DOMAIN",
                "ScopeDescription": "Write to the protected domain of DOMAIN",
            },
        ]

        self.cognito_adapter.add_resource_server_scopes = Mock()
        self.protected_domain_service.append_scopes_to_parameter = Mock()

        self.protected_domain_service.create_scopes("domain")
        self.cognito_adapter.add_resource_server_scopes.assert_called_once_with(
            COGNITO_USER_POOL_ID, COGNITO_RESOURCE_SERVER_ID, expected_scopes
        )
        self.protected_domain_service.append_scopes_to_parameter.assert_called_once_with(
            expected_scopes
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

    def test_append_scopes_to_parameter(self):
        mock_response = '[{"ScopeName": "old", "ScopeDescription": "old"}]'
        new_scope = [{"ScopeName": "new", "ScopeDescription": "new"}]

        self.ssm_adapter.get_parameter = Mock(return_value=mock_response)
        self.ssm_adapter.put_parameter = Mock()

        self.protected_domain_service.append_scopes_to_parameter(new_scope)
        self.ssm_adapter.get_parameter.assert_called_once_with(
            PROTECTED_DOMAIN_SCOPES_PARAMETER_NAME
        )
        self.ssm_adapter.put_parameter.assert_called_once_with(
            PROTECTED_DOMAIN_SCOPES_PARAMETER_NAME,
            '[{"ScopeName": "old", "ScopeDescription": "old"}, {"ScopeName": "new", "ScopeDescription": "new"}]',
        )
