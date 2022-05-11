from unittest.mock import patch

from api.application.services.client_service import ClientService
from api.common.custom_exceptions import UserError, AWSServiceError
from api.domain.client import ClientResponse, ClientRequest
from test.api.controller.controller_test_utils import BaseClientTest


class TestClientCreation(BaseClientTest):

    @patch.object(ClientService, "create_client")
    def test_returns_client_information_when_valid_request(self, mock_create_client):
        expected_response = ClientResponse(
            client_name="my_client",
            client_id="some-client-id",
            client_secret="some-client-secret",  # pragma: allowlist secret
            scopes=["WRITE_PUBLIC", "READ_PRIVATE"])

        mock_create_client.return_value = expected_response

        client_request = ClientRequest(
            client_name="my_client",
            scopes=["WRITE_PUBLIC", "READ_PRIVATE"]
        )

        response = self.client.post(
            "/client",
            headers={"Authorization": "Bearer test-token"},
            json={
                "client_name": "my_client",
                "scopes": ["WRITE_PUBLIC", "READ_PRIVATE"]
            }
        )

        mock_create_client.assert_called_once_with(client_request)

        assert response.status_code == 201
        assert response.json() == expected_response

    @patch.object(ClientService, "create_client")
    def test_accepts_empty_scopes(self, mock_create_client):
        expected_response = ClientResponse(
            client_name="my_client",
            client_id="some-client-id",
            client_secret="some-client-secret",  # pragma: allowlist secret
            scopes=["READ_PUBLIC"])

        mock_create_client.return_value = expected_response

        client_request = ClientRequest(
            client_name="my_client",
            scopes=["READ_PUBLIC"]
        )

        response = self.client.post(
            "/client",
            headers={"Authorization": "Bearer test-token"},
            json={
                "client_name": "my_client"
            }
        )

        mock_create_client.assert_called_once_with(client_request)

        assert response.status_code == 201
        assert response.json() == expected_response

    def test_throws_an_exception_when_client_is_empty(self):
        response = self.client.post(
            "/client",
            headers={"Authorization": "Bearer test-token"},
            json={
                "scopes": ["WRITE_PUBLIC", "READ_PRIVATE"]
            }
        )

        assert response.status_code == 400
        assert response.json() == {"details": ["client_name -> field required"]}

    @patch.object(ClientService, "create_client")
    def test_bad_request_when_invalid_scopes(self, mock_create_client):
        mock_create_client.side_effect = UserError("One or more of the provided scopes do not exist")

        response = self.client.post(
            "/client",
            headers={"Authorization": "Bearer test-token"},
            json={
                "client_name": "my_client",
                "scopes": ["INVALID_SCOPE"]
            }
        )

        assert response.status_code == 400
        assert response.json() == {"details": "One or more of the provided scopes do not exist"}

    @patch.object(ClientService, "create_client")
    def test_internal_error_when_client_creation_fails(self, mock_create_client):
        mock_create_client.side_effect = AWSServiceError("The client 'my_client' could not be created")

        response = self.client.post(
            "/client",
            headers={"Authorization": "Bearer test-token"},
            json={
                "client_name": "my_client",
                "scopes": ["INVALID_SCOPE"]
            }
        )

        assert response.status_code == 500
        assert response.json() == {"details": "The client 'my_client' could not be created"}
