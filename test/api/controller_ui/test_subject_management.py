from unittest.mock import patch, ANY

from fastapi.templating import Jinja2Templates

from api.application.services.permissions_service import PermissionsService
from test.api.common.controller_test_utils import BaseClientTest


class TestSubjectPage(BaseClientTest):
    @patch.object(Jinja2Templates, "TemplateResponse")
    def test_calls_templating_engine_with_expected_arguments(
        self, mock_templates_response
    ):
        subject_template_filename = "subject.html"

        response = self.client.get("/subject", cookies={"rat": "user_token"})

        mock_templates_response.assert_called_once_with(
            name=subject_template_filename,
            context={"request": ANY},
        )

        assert response.status_code == 200


class TestModifySubjectPage(BaseClientTest):
    @patch.object(Jinja2Templates, "TemplateResponse")
    def test_calls_templating_engine_with_expected_arguments(
        self, mock_templates_response
    ):
        subject_template_filename = "subject_modify.html"

        response = self.client.get(
            "/subject/a1b2c3d4/modify", cookies={"rat": "user_token"}
        )

        mock_templates_response.assert_called_once_with(
            name=subject_template_filename,
            context={"request": ANY, "subject_name": "a1b2c3d4"},
        )

        assert response.status_code == 200


class TestCreateSubjectPage(BaseClientTest):
    @patch.object(PermissionsService, "get_ui_permissions")
    @patch.object(Jinja2Templates, "TemplateResponse")
    def test_calls_templating_engine_with_expected_arguments(
        self, mock_templates_response, mock_ui_permissions
    ):
        expected_permission_object = {
            "ADMIN": ["USER_ADMIN", "DATA_ADMIN"],
            "GLOBAL_READ": ["READ_ALL", "READ_PUBLIC", "READ_PRIVATE"],
            "GLOBAL_WRITE": ["WRITE_ALL", "WRITE_PUBLIC", "WRITE_PRIVATE"],
            "PROTECTED_READ": ["READ_PROTECTED_TEST"],
            "PROTECTED_WRITE": ["WRITE_PROTECTED_TEST"],
        }
        subject_template_filename = "subject_create.html"

        mock_ui_permissions.return_value = expected_permission_object

        response = self.client.get("/subject/create", cookies={"rat": "user_token"})

        mock_templates_response.assert_called_once_with(
            name=subject_template_filename,
            context={"request": ANY, "permissions": expected_permission_object},
        )

        mock_ui_permissions.assert_called_once()

        assert response.status_code == 200
