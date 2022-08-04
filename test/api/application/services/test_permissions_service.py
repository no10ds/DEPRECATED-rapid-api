from unittest.mock import Mock

from api.application.services.permissions_service import PermissionsService


class TestGetPermissions:
    def setup_method(self):
        self.dynamo_adapter = Mock()
        self.permissions_service = PermissionsService(self.dynamo_adapter)

    def test_get_permissions(self):
        expected_response = ["WRITE_PUBLIC", "READ_PRIVATE", "DATA_ADMIN", "USER_ADMIN"]
        self.dynamo_adapter.get_all_permissions.return_value = [
            "WRITE_PUBLIC",
            "READ_PRIVATE",
            "DATA_ADMIN",
            "USER_ADMIN",
        ]
        actual_response = self.permissions_service.get_permissions()

        self.dynamo_adapter.get_all_permissions.assert_called_once()
        assert actual_response == expected_response
