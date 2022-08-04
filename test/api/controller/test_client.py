from unittest.mock import patch

from api.application.services.subject_service import SubjectService
from api.common.custom_exceptions import UserError, AWSServiceError
from api.domain.client import ClientResponse, ClientRequest
from api.domain.subject_permissions import SubjectPermissions
from test.api.controller.controller_test_utils import BaseClientTest


class TestClientCreation(BaseClientTest):
    @patch.object(SubjectService, "create_client")
    def test_returns_client_information_when_valid_request(self, mock_create_client):
        expected_response = ClientResponse(
            client_name="my_client",
            client_id="some-client-id",
            client_secret="some-client-secret",  # pragma: allowlist secret
            permissions=["WRITE_PUBLIC", "READ_PRIVATE"],
        )

        mock_create_client.return_value = expected_response

        client_request = ClientRequest(
            client_name="my_client", permissions=["WRITE_PUBLIC", "READ_PRIVATE"]
        )

        response = self.client.post(
            "/client",
            headers={"Authorization": "Bearer test-token"},
            json={
                "client_name": "my_client",
                "permissions": ["WRITE_PUBLIC", "READ_PRIVATE"],
            },
        )

        mock_create_client.assert_called_once_with(client_request)

        assert response.status_code == 201
        assert response.json() == expected_response

    @patch.object(SubjectService, "create_client")
    def test_accepts_empty_permissions(self, mock_create_client):
        expected_response = ClientResponse(
            client_name="my_client",
            client_id="some-client-id",
            client_secret="some-client-secret",  # pragma: allowlist secret
            permissions=["READ_PUBLIC"],
        )

        mock_create_client.return_value = expected_response

        client_request = ClientRequest(
            client_name="my_client", permissions=["READ_PUBLIC"]
        )

        response = self.client.post(
            "/client",
            headers={"Authorization": "Bearer test-token"},
            json={"client_name": "my_client"},
        )

        mock_create_client.assert_called_once_with(client_request)

        assert response.status_code == 201
        assert response.json() == expected_response

    def test_throws_an_exception_when_client_is_empty(self):
        response = self.client.post(
            "/client",
            headers={"Authorization": "Bearer test-token"},
            json={"permissions": ["WRITE_PUBLIC", "READ_PRIVATE"]},
        )

        assert response.status_code == 400
        assert response.json() == {"details": ["client_name -> field required"]}

    @patch.object(SubjectService, "create_client")
    def test_bad_request_when_invalid_permissions(self, mock_create_client):
        mock_create_client.side_effect = UserError(
            "One or more of the provided permissions do not exist"
        )

        response = self.client.post(
            "/client",
            headers={"Authorization": "Bearer test-token"},
            json={"client_name": "my_client", "permissions": ["INVALID_SCOPE"]},
        )

        assert response.status_code == 400
        assert response.json() == {
            "details": "One or more of the provided permissions do not exist"
        }

    @patch.object(SubjectService, "create_client")
    def test_internal_error_when_client_creation_fails(self, mock_create_client):
        mock_create_client.side_effect = AWSServiceError(
            "The client 'my_client' could not be created"
        )

        response = self.client.post(
            "/client",
            headers={"Authorization": "Bearer test-token"},
            json={"client_name": "my_client", "permissions": ["INVALID_SCOPE"]},
        )

        assert response.status_code == 500
        assert response.json() == {
            "details": "The client 'my_client' could not be created"
        }


class TestModifyClientPermissions(BaseClientTest):
    @patch.object(SubjectService, "set_subject_permissions")
    def test_update_client_permissions(self, mock_set_subject_permissions):
        subject_id = "asdf1243kj456"
        new_permissions = ["READ_ALL", "WRITE_ALL"]

        mock_set_subject_permissions.return_value = {
            "subject_id": subject_id,
            "permissions": new_permissions,
        }

        subject_permissions = SubjectPermissions(
            subject_id=subject_id, permissions=new_permissions
        )

        response = self.client.put(
            "/client/permissions",
            headers={"Authorization": "Bearer test-token"},
            json={
                "subject_id": subject_permissions.subject_id,
                "permissions": subject_permissions.permissions,
            },
        )

        assert response.status_code == 200
        assert response.json() == {
            "subject_id": subject_permissions.subject_id,
            "permissions": subject_permissions.permissions,
        }
        mock_set_subject_permissions.assert_called_once_with(subject_permissions)

    @patch.object(SubjectService, "set_subject_permissions")
    def test_bad_request_when_invalid_permissions(self, mock_set_subject_permissions):
        mock_set_subject_permissions.side_effect = UserError("Invalid permissions")

        response = self.client.put(
            "/client/permissions",
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
            "/client/permissions",
            headers={"Authorization": "Bearer test-token"},
            json={
                "subject_id": "1234",
                "permissions": ["permission1", "permission2"],
            },
        )

        assert response.status_code == 500
        assert response.json() == {"details": "Database error"}


class TestClientDeletion(BaseClientTest):
    @patch.object(SubjectService, "delete_client")
    def test_returns_client_information_when_valid_request(self, mock_delete_client):
        expected_response = {"message": "The client 'my-client-id' has been deleted"}

        response = self.client.delete(
            "/client/my-client-id",
            headers={"Authorization": "Bearer test-token"},
        )

        mock_delete_client.assert_called_once_with("my-client-id")

        assert response.status_code == 200
        assert response.json() == expected_response

    @patch.object(SubjectService, "delete_client")
    def test_bad_request_when_client_does_not_exist(self, mock_delete_client):
        mock_delete_client.side_effect = UserError(
            "The client 'my-client-id' does not exist cognito"
        )

        response = self.client.delete(
            "/client/my-client-id",
            headers={"Authorization": "Bearer test-token"},
        )

        assert response.status_code == 400
        assert response.json() == {
            "details": "The client 'my-client-id' does not exist cognito"
        }

    @patch.object(SubjectService, "delete_client")
    def test_internal_error_when_client_deletion_fails(self, mock_delete_client):
        mock_delete_client.side_effect = AWSServiceError(
            "Something went wrong. Please Contact your administrator."
        )

        response = self.client.delete(
            "/client/my-client-id",
            headers={"Authorization": "Bearer test-token"},
        )

        assert response.status_code == 500
        assert response.json() == {
            "details": "Something went wrong. Please Contact your administrator."
        }
