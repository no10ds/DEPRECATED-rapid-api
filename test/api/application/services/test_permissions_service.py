from unittest.mock import Mock

from api.application.services.permissions_service import PermissionsService
from api.common.custom_exceptions import AWSServiceError


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


class TestGetSubjectPermissions:
    def setup_method(self):
        self.dynamo_adapter = Mock()
        self.permissions_service = PermissionsService(self.dynamo_adapter)

    def test_get_permissions(self):
        subject_id = "123abc"
        expected_response = ["WRITE_PUBLIC", "READ_PRIVATE", "DATA_ADMIN", "USER_ADMIN"]
        self.dynamo_adapter.get_permissions_for_subject.return_value = expected_response

        actual_response = self.permissions_service.get_subject_permissions(subject_id)

        self.dynamo_adapter.get_permissions_for_subject.assert_called_once_with(
            subject_id
        )
        assert actual_response == expected_response


class TestGetUIPermissions:
    def setup_method(self):
        self.dynamo_adapter = Mock()
        self.permissions_service = PermissionsService(self.dynamo_adapter)

    def test_gets_all_permissions_for_ui(self):
        all_permissions = [
            "WRITE_ALL",
            "WRITE_PUBLIC",
            "WRITE_PRIVATE",
            "READ_PRIVATE",
            "USER_ADMIN",
            "DATA_ADMIN",
            "READ_PROTECTED_SOME_DOMAIN",
            "WRITE_PROTECTED_SOME_DOMAIN",
        ]

        self.dynamo_adapter.get_all_permissions.return_value = all_permissions

        expected = {
            "ADMIN": [
                {"name": "USER_ADMIN", "display_name": "USER_ADMIN"},
                {"name": "DATA_ADMIN", "display_name": "DATA_ADMIN"},
            ],
            "GLOBAL_READ": [{"name": "READ_PRIVATE", "display_name": "READ_PRIVATE"}],
            "GLOBAL_WRITE": [
                {"name": "WRITE_ALL", "display_name": "WRITE_ALL"},
                {"name": "WRITE_PUBLIC", "display_name": "WRITE_PUBLIC"},
                {"name": "WRITE_PRIVATE", "display_name": "WRITE_PRIVATE"},
            ],
            "PROTECTED_READ": [
                {
                    "name": "READ_PROTECTED_SOME_DOMAIN",
                    "display_name": "READ_PROTECTED_SOME_DOMAIN",
                }
            ],
            "PROTECTED_WRITE": [
                {
                    "name": "WRITE_PROTECTED_SOME_DOMAIN",
                    "display_name": "WRITE_PROTECTED_SOME_DOMAIN",
                }
            ],
        }

        result = self.permissions_service.get_all_permissions_ui()

        assert result == expected

    def test_sends_empty_permissions_if_error_in_retrieving_from_db(self):
        self.dynamo_adapter.get_all_permissions.side_effect = AWSServiceError(
            "the error"
        )

        expected = {
            "ADMIN": [],
            "GLOBAL_READ": [],
            "GLOBAL_WRITE": [],
            "PROTECTED_READ": [],
            "PROTECTED_WRITE": [],
        }

        result = self.permissions_service.get_all_permissions_ui()

        assert result == expected

    def test_gets_user_permissions_for_ui(self):
        all_permissions = [
            "WRITE_ALL",
            "WRITE_PUBLIC",
            "WRITE_PRIVATE",
            "READ_PRIVATE",
            "USER_ADMIN",
            "DATA_ADMIN",
            "READ_PROTECTED_SOME_DOMAIN",
            "WRITE_PROTECTED_SOME_DOMAIN",
        ]

        self.dynamo_adapter.get_all_permissions.return_value = all_permissions

        expected = {
            "ADMIN": [
                {"name": "USER_ADMIN", "display_name": "USER_ADMIN"},
                {"name": "DATA_ADMIN", "display_name": "DATA_ADMIN"},
            ],
            "GLOBAL_READ": [{"name": "READ_PRIVATE", "display_name": "READ_PRIVATE"}],
            "GLOBAL_WRITE": [
                {"name": "WRITE_ALL", "display_name": "WRITE_ALL"},
                {"name": "WRITE_PUBLIC", "display_name": "WRITE_PUBLIC"},
                {"name": "WRITE_PRIVATE", "display_name": "WRITE_PRIVATE"},
            ],
            "PROTECTED_READ": [
                {
                    "name": "READ_PROTECTED_SOME_DOMAIN",
                    "display_name": "READ_PROTECTED_SOME_DOMAIN",
                }
            ],
            "PROTECTED_WRITE": [
                {
                    "name": "WRITE_PROTECTED_SOME_DOMAIN",
                    "display_name": "WRITE_PROTECTED_SOME_DOMAIN",
                }
            ],
        }

        result = self.permissions_service.get_all_permissions_ui()

        assert result == expected

    def test_sends_empty_user_permissions_if_error_in_retrieving_from_db(self):
        self.dynamo_adapter.get_permissions_for_subject.side_effect = AWSServiceError(
            "the error"
        )

        expected = {
            "ADMIN": [],
            "GLOBAL_READ": [],
            "GLOBAL_WRITE": [],
            "PROTECTED_READ": [],
            "PROTECTED_WRITE": [],
        }

        result = self.permissions_service.get_user_permissions_ui("the-subject-id")

        self.dynamo_adapter.get_permissions_for_subject.assert_called_once_with(
            "the-subject-id"
        )
        assert result == expected
