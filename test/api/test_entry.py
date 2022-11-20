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
        response = self.client.get("/status")
        assert response.status_code == 200

    def test_returns_metadata_for_api(self):
        response = self.client.get("/apis")
        expected_response = {
            "api-version": "api.gov.uk/v1alpha",
            "apis": [
                {
                    "api-version": "api.gov.uk/v1alpha",
                    "data": {
                        "name": "Project rAPId",
                        "description": "Sample rAPId description",
                        "url": "https://getrapid.link/docs",
                        "contact": "rapid@no10.gov.uk",
                        "organisation": "10 Downing Street & Cabinet Office",
                        "documentation-url": "https://github.com/no10ds/rapid-api",
                    }
                }
            ]
        }
        assert response.status_code == 200
        assert response.json() == expected_response