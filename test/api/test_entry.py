from unittest.mock import patch

import pytest

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
