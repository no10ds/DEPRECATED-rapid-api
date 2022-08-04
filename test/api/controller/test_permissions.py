from unittest.mock import patch

from api.application.services.permissions_service import PermissionsService
from test.api.controller.controller_test_utils import BaseClientTest


class TestListPermissions(BaseClientTest):
    @patch.object(PermissionsService, "get_permissions")
    def test_it_returns_a_list_of_permissions(self, mock_get_permissions):
        expected_response = ["WRITE_PUBLIC", "READ_PRIVATE", "DATA_ADMIN", "USER_ADMIN"]
        mock_get_permissions.return_value = expected_response

        actual_response = self.client.get("/permissions")

        mock_get_permissions.assert_called_once()

        assert actual_response.status_code == 200
        assert actual_response.json() == expected_response
