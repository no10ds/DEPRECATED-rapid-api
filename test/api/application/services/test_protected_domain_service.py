from unittest.mock import Mock
from api.application.services.protected_domain_service import ProtectedDomainService

from api.common.config.auth import COGNITO_USER_POOL_ID, COGNITO_RESOURCE_SERVER_ID


class TestProtectedDomainService:
    def test_create_private_domain_scope(self):
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

        protected_domain_service.cognito_adapter.add_resource_server_scopes = Mock()
        protected_domain_service.create_scopes("domain")
        protected_domain_service.cognito_adapter.add_resource_server_scopes.assert_called_once_with(
            COGNITO_USER_POOL_ID, COGNITO_RESOURCE_SERVER_ID, expected_scopes
        )
