import pytest
from unittest.mock import Mock
from typing import List

from api.application.services.protected_domain_service import ProtectedDomainService
from api.common.config.auth import COGNITO_USER_POOL_ID, COGNITO_RESOURCE_SERVER_ID


class TestProtectedDomainService:
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

        protected_domain_service = ProtectedDomainService(Mock())

        protected_domain_service.cognito_adapter.update_resource_server_scopes = Mock()
        protected_domain_service.create_scopes("domain")
        protected_domain_service.cognito_adapter.update_resource_server_scopes.assert_called_once_with(
            COGNITO_USER_POOL_ID, COGNITO_RESOURCE_SERVER_ID, expected_scopes
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

        protected_domain_service = ProtectedDomainService(Mock())
        protected_domain_service.cognito_adapter.get_resource_server = Mock(
            return_value=mock_response
        )

        res = protected_domain_service.list_domains()
        assert res == sorted(expected_response)
        protected_domain_service.cognito_adapter.get_resource_server.assert_called_once_with(
            COGNITO_USER_POOL_ID, COGNITO_RESOURCE_SERVER_ID
        )
