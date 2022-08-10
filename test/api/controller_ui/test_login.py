import urllib.parse
from unittest.mock import patch, ANY

from fastapi.templating import Jinja2Templates
from starlette.status import HTTP_302_FOUND

from api.common.config.auth import (
    IDENTITY_PROVIDER_AUTHORIZATION_URL,
    COGNITO_REDIRECT_URI,
    RAPID_ACCESS_TOKEN,
)
from test.api.common.controller_test_utils import BaseClientTest


class TestLoginPage(BaseClientTest):
    @patch("api.controller_ui.login.user_logged_in")
    @patch("api.controller_ui.login.get_secret")
    @patch.object(Jinja2Templates, "TemplateResponse")
    def test_calls_templating_engine_with_expected_arguments(
        self, mock_templates_response, mock_get_secret, mock_log_in
    ):
        mock_log_in.return_value = False
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

    @patch("api.controller_ui.login.user_logged_in")
    @patch("api.controller_ui.login.RedirectResponse")
    def test_redirects_to_landing_when_the_user_has_logged_in_already(
        self, mock_redirect_response, mock_log_in
    ):
        mock_log_in.return_value = True
        self.client.get("/login")
        mock_redirect_response.assert_called_once_with(
            url="/", status_code=HTTP_302_FOUND
        )


class TestLogoutPage(BaseClientTest):
    @patch("api.controller_ui.login.construct_logout_url")
    @patch("api.controller_ui.login.get_secret")
    @patch("api.controller_ui.login.RedirectResponse.delete_cookie")
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
