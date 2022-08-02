from unittest.mock import patch

from api.application.services.subject_service import SubjectService
from api.common.custom_exceptions import UserError, AWSServiceError
from api.domain.subject_permissions import SubjectPermissions
from api.domain.user import UserResponse, UserRequest
from test.api.controller.controller_test_utils import BaseClientTest


class TestUserCreation(BaseClientTest):
    @patch.object(SubjectService, "create_user")
    def test_returns_user_information_when_valid_request(self, mock_create_user):
        expected_response = UserResponse(
            username="user-name",
            email="user-name@some-email.com",
            permissions=["WRITE_PUBLIC", "READ_PRIVATE"],
            user_id="some-uu-id-b226-e5fd18c59b85",
        )

        mock_create_user.return_value = expected_response

        user_request = UserRequest(
            username="user-name",
            email="user-name@some-email.com",
            permissions=["WRITE_PUBLIC", "READ_PRIVATE"],
        )

        response = self.client.post(
            "/user",
            headers={"Authorization": "Bearer test-token"},
            json={
                "username": "user-name",
                "email": "user-name@some-email.com",
                "permissions": ["WRITE_PUBLIC", "READ_PRIVATE"],
            },
        )

        mock_create_user.assert_called_once_with(user_request)

        assert response.status_code == 201
        assert response.json() == expected_response

    @patch.object(SubjectService, "create_user")
    def test_accepts_empty_permissions(self, mock_create_user):
        expected_response = UserResponse(
            username="user-name",
            email="user-name@some-email.com",
            permissions=["READ_PUBLIC"],
            user_id="some-uu-id-b226-e5fd18c59b85",
        )

        mock_create_user.return_value = expected_response

        user_request = UserRequest(
            username="user-name",
            email="user-name@some-email.com",
            permissions=["READ_PUBLIC"],
        )

        response = self.client.post(
            "/user",
            headers={"Authorization": "Bearer test-token"},
            json={
                "username": "user-name",
                "email": "user-name@some-email.com",
            },
        )

        mock_create_user.assert_called_once_with(user_request)

        assert response.status_code == 201
        assert response.json() == expected_response

    def test_throws_an_exception_when_user_is_empty(self):
        response = self.client.post(
            "/user",
            headers={"Authorization": "Bearer test-token"},
            json={"permissions": ["WRITE_PUBLIC", "READ_PRIVATE"]},
        )

        assert response.status_code == 400
        assert response.json() == {
            "details": ["username -> field required", "email -> field required"]
        }

    @patch.object(SubjectService, "create_user")
    def test_bad_request_when_invalid_permissions(self, mock_create_user):
        mock_create_user.side_effect = UserError(
            "One or more of the provided permissions do not exist"
        )

        response = self.client.post(
            "/user",
            headers={"Authorization": "Bearer test-token"},
            json={
                "username": "my_user",
                "email": "email@email.com",
                "permissions": ["INVALID_SCOPE"],
            },
        )

        assert response.status_code == 400
        assert response.json() == {
            "details": "One or more of the provided permissions do not exist"
        }

    @patch.object(SubjectService, "create_user")
    def test_internal_error_when_user_creation_fails(self, mock_create_user):
        mock_create_user.side_effect = AWSServiceError(
            "The user 'my_user' could not be created"
        )

        response = self.client.post(
            "/user",
            headers={"Authorization": "Bearer test-token"},
            json={
                "username": "my_user",
                "email": "email@email.com",
                "permissions": ["INVALID_SCOPE"],
            },
        )

        assert response.status_code == 500
        assert response.json() == {"details": "The user 'my_user' could not be created"}


class TestModifyUserPermissions(BaseClientTest):
    @patch.object(SubjectService, "set_subject_permissions")
    def test_update_user_permissions(self, mock_set_subject_permissions):
        subject_permissions = SubjectPermissions(
            subject_id="asdf1243kj456", permissions=["READ_ALL", "WRITE_ALL"]
        )

        response = self.client.put(
            "/user/permissions",
            headers={"Authorization": "Bearer test-token"},
            json={
                "subject_id": subject_permissions.subject_id,
                "permissions": subject_permissions.permissions,
            },
        )

        assert response.status_code == 200
        mock_set_subject_permissions.assert_called_once_with(subject_permissions)

    @patch.object(SubjectService, "set_subject_permissions")
    def test_bad_request_when_invalid_permissions(self, mock_set_subject_permissions):
        mock_set_subject_permissions.side_effect = UserError("Invalid permissions")

        response = self.client.put(
            "/user/permissions",
            headers={"Authorization": "Bearer test-token"},
            json={
                "subject_id": "1234",
                "permissions": ["permission1", "permission2"],
            },
        )

        assert response.status_code == 400
        assert response.json() == {"details": "Invalid permissions"}

    @patch.object(SubjectService, "set_subject_permissions")
    def test_internal_error_when_invalid_permissions(
        self, mock_set_subject_permissions
    ):
        mock_set_subject_permissions.side_effect = AWSServiceError("Database error")

        response = self.client.put(
            "/user/permissions",
            headers={"Authorization": "Bearer test-token"},
            json={
                "subject_id": "1234",
                "permissions": ["permission1", "permission2"],
            },
        )

        assert response.status_code == 500
        assert response.json() == {"details": "Database error"}
