from unittest.mock import patch, ANY, Mock

import pytest
from fastapi.templating import Jinja2Templates

from api.common.custom_exceptions import UserError, AWSServiceError
from api.controller_ui.landing import determine_user_ui_actions
from api.common.config.constants import BASE_API_PATH
from test.api.common.controller_test_utils import BaseClientTest


class TestLandingPage(BaseClientTest):
    @pytest.mark.parametrize(
        "permissions,can_manage_users,can_upload,can_download,can_create_schema",
        [
            ([], False, False, False, False),
            (["READ_ALL"], False, False, True, False),
            (["WRITE_ALL"], False, True, False, False),
            (["DATA_ADMIN"], False, False, False, True),
            (["USER_ADMIN"], True, False, False, False),
            (["READ_ALL", "WRITE_ALL"], False, True, True, False),
            (["USER_ADMIN", "READ_ALL", "WRITE_ALL"], True, True, True, False),
            (["READ_PRIVATE", "WRITE_PUBLIC"], False, True, True, False),
            (
                ["READ_PROTECTED_domain1", "WRITE_PROTECTED_domain2"],
                False,
                True,
                True,
                False,
            ),
        ],
    )
    def test_determines_user_allowed_ui_actions(
        self, permissions, can_manage_users, can_upload, can_download, can_create_schema
    ):
        allowed_actions = determine_user_ui_actions(permissions)

        assert allowed_actions["can_manage_users"] is can_manage_users
        assert allowed_actions["can_upload"] is can_upload
        assert allowed_actions["can_download"] is can_download
        assert allowed_actions["can_create_schema"] is can_create_schema

    @patch("api.controller_ui.landing.permissions_service")
    @patch("api.controller_ui.landing.parse_token")
    @patch.object(Jinja2Templates, "TemplateResponse")
    def test_calls_template_with_expected_arguments(
        self, mock_templates_response, mock_parse_token, mock_permissions_service
    ):
        landing_template_filename = "index.html"
        mock_token = Mock()
        mock_token.subject = "123abc"
        mock_parse_token.return_value = mock_token

        mock_permissions_service.get_subject_permissions.return_value = [
            "READ_ALL",
            "WRITE_ALL",
            "USER_ADMIN",
            "DATA_ADMIN",
        ]

        response = self.client.get(f"{BASE_API_PATH}/", cookies={"rat": "user_token"})

        mock_templates_response.assert_called_once_with(
            name=landing_template_filename,
            context={
                "request": ANY,
                "error_message": None,
                "can_manage_users": True,
                "can_upload": True,
                "can_download": True,
                "can_create_schema": True,
            },
        )

        assert response.status_code == 200

    @patch("api.controller_ui.landing.permissions_service")
    @patch("api.controller_ui.landing.parse_token")
    @patch.object(Jinja2Templates, "TemplateResponse")
    def test_calls_template_with_expected_arguments_when_user_error(
        self, mock_templates_response, mock_parse_token, mock_permissions_service
    ):
        landing_template_filename = "index.html"
        mock_token = Mock()
        mock_token.subject = "123abc"
        mock_parse_token.return_value = mock_token

        mock_permissions_service.get_subject_permissions.side_effect = UserError(
            "a message"
        )

        response = self.client.get(f"{BASE_API_PATH}/", cookies={"rat": "user_token"})

        mock_templates_response.assert_called_once_with(
            name=landing_template_filename,
            context={
                "request": ANY,
                "error_message": "You have not been granted relevant permissions. Please speak to your system administrator.",
            },
        )

        assert response.status_code == 200

    @patch("api.controller_ui.landing.permissions_service")
    @patch("api.controller_ui.landing.parse_token")
    @patch.object(Jinja2Templates, "TemplateResponse")
    def test_calls_template_with_expected_arguments_when_aws_error(
        self, mock_templates_response, mock_parse_token, mock_permissions_service
    ):
        landing_template_filename = "index.html"
        mock_token = Mock()
        mock_token.subject = "123abc"
        mock_parse_token.return_value = mock_token

        mock_permissions_service.get_subject_permissions.side_effect = AWSServiceError(
            "a custom message"
        )

        response = self.client.get(f"{BASE_API_PATH}/", cookies={"rat": "user_token"})

        mock_templates_response.assert_called_once_with(
            name=landing_template_filename,
            context={"request": ANY, "error_message": "a custom message"},
        )

        assert response.status_code == 200

    @patch("api.controller_ui.landing.determine_user_ui_actions")
    @patch("api.controller_ui.landing.permissions_service")
    @patch("api.controller_ui.landing.parse_token")
    @patch.object(Jinja2Templates, "TemplateResponse")
    def test_calls_template_with_expected_arguments_when_no_permissions(
        self,
        mock_templates_response,
        mock_parse_token,
        _mock_permissions_service,
        mock_ui_actions,
    ):
        landing_template_filename = "index.html"
        mock_token = Mock()
        mock_token.subject = "123abc"
        mock_parse_token.return_value = mock_token

        mock_ui_actions.return_value = {}

        response = self.client.get(f"{BASE_API_PATH}/", cookies={"rat": "user_token"})

        mock_templates_response.assert_called_once_with(
            name=landing_template_filename,
            context={
                "request": ANY,
                "error_message": "You have not been granted relevant permissions. Please speak to your system administrator.",
            },
        )

        assert response.status_code == 200
