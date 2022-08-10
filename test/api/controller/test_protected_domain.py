from unittest.mock import patch, Mock

from api.application.services.protected_domain_service import ProtectedDomainService
from test.api.common.controller_test_utils import BaseClientTest


class TestProtectedDomains(BaseClientTest):
    @patch.object(ProtectedDomainService, "create_protected_domain_permission")
    def test_scopes_creation(self, create_protected_domain_permission: Mock):
        response = self.client.post(
            "/protected_domains/new",
            headers={"Authorization": "Bearer test-token"},
        )

        create_protected_domain_permission.assert_called_once_with("new")

        assert response.status_code == 201
        assert response.json() == {
            "message": "Successfully created protected domain for new"
        }

    @patch.object(ProtectedDomainService, "list_protected_domains")
    def test_list_protected_domains(self, list_protected_domains: Mock):
        list_protected_domains.return_value = ["test"]

        response = self.client.get(
            "/protected_domains",
            headers={"Authorization": "Bearer test-token"},
        )

        list_protected_domains.assert_called_once()

        assert response.status_code == 200
        assert response.json() == ["test"]
