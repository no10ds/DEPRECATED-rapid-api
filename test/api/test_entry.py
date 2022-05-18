import urllib.parse
from unittest.mock import patch, ANY

from fastapi.templating import Jinja2Templates

from api.application.services.authorisation_service import RAPID_ACCESS_TOKEN
from api.common.config.auth import (
    IDENTITY_PROVIDER_AUTHORIZATION_URL,
    COGNITO_REDIRECT_URI,
)
from test.api.controller.controller_test_utils import BaseClientTest


class TestStatus(BaseClientTest):
    def test_http_status_response_is_200_status(self):
        response = self.client.get("/status")
        assert response.status_code == 200


class TestLoginPage(BaseClientTest):
    @patch("api.entry.get_secret")
    @patch.object(Jinja2Templates, "TemplateResponse")
    def test_calls_templating_engine_with_expected_arguments(
        self, mock_templates_response, mock_get_secret
    ):
        mock_get_secret.return_value = {"client_id": "12345", "client_secret": "54321"}
        login_template_filename = "login.html"

        response = self.client.get("/login")

        mock_templates_response.assert_called_once_with(
            name=login_template_filename,
            context={
                "request": ANY,
                "auth_url": f"{IDENTITY_PROVIDER_AUTHORIZATION_URL}?client_id=12345&response_type=code&redirect_uri={urllib.parse.quote_plus(COGNITO_REDIRECT_URI)}",
            },
        )

        assert response.status_code == 200


class TestUploadPage(BaseClientTest):
    @patch("api.entry.extract_user_groups")
    @patch.object(Jinja2Templates, "TemplateResponse")
    def test_calls_templating_engine_with_expected_arguments(
        self, mock_templates_response, mock_extract_users
    ):
        login_template_filename = "upload.html"
        mock_extract_users.return_value = [
            "WRITE/domain1/dataset1",
            "WRITE/domain2/dataset2",
            "USER_ADMIN",
            "READ/domain2/dataset2",
        ]

        response = self.client.get("/upload", cookies={"rat": "some_token"})

        mock_templates_response.assert_called_once_with(
            name=login_template_filename,
            context={
                "request": ANY,
                "datasets": ["domain1/dataset1", "domain2/dataset2"],
            },
        )

        assert response.status_code == 200


class TestLogoutPage(BaseClientTest):
    @patch("api.entry.RedirectResponse.delete_cookie")
    def test_logs_out_the_user(self, mock_delete_cookie):
        response = self.client.get("/logout", allow_redirects=False)
        mock_delete_cookie.assert_called_once_with(RAPID_ACCESS_TOKEN)
        assert response.status_code == 302
        assert response.is_redirect
        assert response.headers.get("location") == "/login"
