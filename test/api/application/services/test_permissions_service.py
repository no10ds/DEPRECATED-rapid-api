from unittest.mock import Mock

from api.application.services.permissions_service import PermissionsService
from api.domain.permission_item import PermissionItem


class TestGetPermissions:
    def setup_method(self):
        self.dynamo_adapter = Mock()
        self.permissions_service = PermissionsService(self.dynamo_adapter)

    def test_get_permissions(self):
        expected_response = [
            "WRITE_ALL_PUBLIC",
            "READ_ALL_PRIVATE",
            "DATA_ADMIN",
            "USER_ADMIN",
        ]
        self.dynamo_adapter.get_all_permissions.return_value = [
            PermissionItem(
                id="WRITE_ALL_PUBLIC",
                type="WRITE",
                layer="ALL",
                sensitivity="PUBLIC",
            ),
            PermissionItem(
                id="READ_ALL_PRIVATE",
                type="READ",
                layer="ALL",
                sensitivity="PRIVATE",
            ),
            PermissionItem(id="DATA_ADMIN", type="DATA_ADMIN"),
            PermissionItem(id="USER_ADMIN", type="USER_ADMIN"),
        ]
        actual_response = self.permissions_service.get_permissions()

        self.dynamo_adapter.get_all_permissions.assert_called_once()
        assert actual_response == expected_response


class TestGetSubjectPermissions:
    def setup_method(self):
        self.dynamo_adapter = Mock()
        self.permissions_service = PermissionsService(self.dynamo_adapter)

    def test_get_permissions(self):
        subject_id = "123abc"
        expected_response = [
            {
                "id": "WRITE_ALL_PUBLIC",
                "type": "WRITE",
                "layer": "ALL",
                "sensitivity": "PUBLIC",
                "domain": None,
            },
            {
                "id": "READ_ALL_PRIVATE",
                "type": "READ",
                "layer": "ALL",
                "sensitivity": "PRIVATE",
                "domain": None,
            },
        ]
        self.dynamo_adapter.get_permission_keys_for_subject.return_value = [
            "WRITE_ALL_PUBLIC",
            "READ_ALL_PRIVATE",
        ]
        self.dynamo_adapter.get_all_permissions.return_value = [
            PermissionItem(
                id="WRITE_ALL_PUBLIC",
                type="WRITE",
                layer="ALL",
                sensitivity="PUBLIC",
            ),
            PermissionItem(
                id="READ_ALL_PRIVATE",
                type="READ",
                layer="ALL",
                sensitivity="PRIVATE",
            ),
            PermissionItem(
                id="DATA_ADMIN",
                type="DATA_ADMIN",
            ),
        ]
        actual_response = self.permissions_service.get_subject_permissions(subject_id)

        self.dynamo_adapter.get_permission_keys_for_subject.assert_called_once_with(
            subject_id
        )
        assert actual_response == expected_response


class TestGetUIPermissions:
    def setup_method(self):
        self.dynamo_adapter = Mock()
        self.permissions_service = PermissionsService(self.dynamo_adapter)

    def test_gets_all_permissions_for_ui(self):
        all_permissions = [
            PermissionItem(
                id="WRITE_ALL",
                type="WRITE",
                layer="ALL",
                sensitivity="ALL",
            ),
            PermissionItem(
                id="READ_ALL_PROTECTED_DOMAIN",
                type="READ",
                layer="ALL",
                sensitivity="PROTECTED",
                domain="domain",
            ),
        ]

        self.dynamo_adapter.get_all_permissions.return_value = all_permissions

        expected = [
            {
                "id": "WRITE_ALL",
                "type": "WRITE",
                "layer": "ALL",
                "sensitivity": "ALL",
                "domain": None,
            },
            {
                "id": "READ_ALL_PROTECTED_DOMAIN",
                "type": "READ",
                "layer": "ALL",
                "sensitivity": "PROTECTED",
                "domain": "domain",
            },
        ]

        result = self.permissions_service.get_all_permissions_ui()

        assert result == expected
