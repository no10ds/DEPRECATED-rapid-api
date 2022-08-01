from unittest.mock import patch

import pytest

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


class TestClientPermissions(BaseClientTest):
    @patch.object(SubjectService, "set_subject_permissions")
    def test_update_client_permissions(self, mock_set_subject_permissions):
        subject_permissions = SubjectPermissions(
            subject_id="asdf1243kj456", permissions=["READ_ALL", "WRITE_ALL"]
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
        mock_set_subject_permissions.assert_called_once_with(subject_permissions)

    @pytest.mark.skip("Not implemented yet")
    @patch.object(SubjectService, "set_subject_permissions")
    def test_update_client_permissions_for_non_user_admin(self, mock_subject_service):
        pass
