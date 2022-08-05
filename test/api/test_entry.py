import urllib.parse
from unittest.mock import patch, ANY, Mock

from fastapi.templating import Jinja2Templates

from api.application.services.UploadService import UploadService
from api.application.services.authorisation.authorisation_service import (
    RAPID_ACCESS_TOKEN,
)
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
    @patch("api.entry.parse_token")
    @patch.object(UploadService, "get_authorised_datasets")
    @patch.object(Jinja2Templates, "TemplateResponse")
    def test_calls_templating_engine_with_expected_arguments(
        self, mock_templates_response, mock_upload_service, mock_parse_token
    ):
        login_template_filename = "upload.html"
        datasets = ["dataset.csv", "dataset2.csv"]
        subject_id = "subject_id"

        mock_token = Mock()
        mock_token.subject = subject_id
        mock_parse_token.return_value = mock_token
        mock_upload_service.return_value = datasets

        response = self.client.get("/upload", cookies={"rat": "user_token"})

        mock_parse_token.assert_called_once_with("user_token")
        mock_upload_service.assert_called_once_with(subject_id)
        mock_templates_response.assert_called_once_with(
            name=login_template_filename,
            context={
                "request": ANY,
                "datasets": datasets,
            },
        )

        assert response.status_code == 200


class TestLogoutPage(BaseClientTest):
    @patch("api.entry.construct_logout_url")
    @patch("api.entry.get_secret")
    @patch("api.entry.RedirectResponse.delete_cookie")
    def test_logs_out_the_user(
        self, mock_delete_cookie, mock_get_secret, mock_construct_logout_url
    ):
        mock_get_secret.return_value = {"client_id": "the-client_id"}
        mock_construct_logout_url.return_value = "https://the-redirect-url.com"

        response = self.client.get("/logout", allow_redirects=False)

        mock_delete_cookie.assert_called_once_with(RAPID_ACCESS_TOKEN)
        mock_construct_logout_url.assert_called_once_with("the-client_id")

        assert response.status_code == 302
        assert response.is_redirect
        assert response.headers.get("location") == "https://the-redirect-url.com"
