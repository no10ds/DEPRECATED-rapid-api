from unittest.mock import patch, Mock

import pytest
from api.application.services.dataset_service import DatasetService
from api.common.config.auth import Action
from api.common.config.constants import BASE_API_PATH

from test.api.common.controller_test_utils import BaseClientTest


@pytest.fixture(scope="session", autouse=True)
def get_client_token_mock():
    with patch("api.entry.get_client_token", return_value=None) as client_token_mock:
        yield client_token_mock


@pytest.fixture(scope="session", autouse=True)
def get_user_token_mock():
    with patch("api.entry.get_user_token", return_value=None) as user_token_mock:
        yield user_token_mock


class TestStatus(BaseClientTest):
    def test_http_status_response_is_200_status(self):
        response = self.client.get("/api/status")
        assert response.status_code == 200

    def test_returns_no_metadata_for_api(self):
        response = self.client.get("/api/apis")
        assert response.status_code == 404

    @patch("api.entry.permissions_service")
    def test_gets_permissions_for_ui(self, mock_permissions_service):
        expected_permission_object = {"any-key": "any-value"}

        mock_permissions_service.get_all_permissions_ui.return_value = (
            expected_permission_object
        )

        response = self.client.get(
            f"{BASE_API_PATH}/permissions_ui", cookies={"rat": "user_token"}
        )

        mock_permissions_service.get_all_permissions_ui.assert_called_once()
        assert response.status_code == 200

    @patch("api.entry.parse_token")
    @patch.object(DatasetService, "get_authorised_datasets")
    def test_gets_datasets_for_ui(self, mock_get_authorised_datasets, mock_parse_token):
        subject_id = "123abc"
        mock_token = Mock()
        mock_token.subject = subject_id
        mock_parse_token.return_value = mock_token

        mock_get_authorised_datasets.return_value = [
            "domain1/datset1/1",
            "domain1/datset2/1",
            "domain2/dataset3/1",
        ]

        response = self.client.get(
            f"{BASE_API_PATH}/datasets_ui", cookies={"rat": "user_token"}
        )

        mock_get_authorised_datasets.assert_called_once_with(subject_id, Action.WRITE)
        assert response.status_code == 200
