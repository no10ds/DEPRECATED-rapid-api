from unittest.mock import patch, ANY

from fastapi.templating import Jinja2Templates

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
    @patch("api.controller_ui.subject_management.permissions_service")
    def test_calls_templating_engine_with_expected_arguments(
        self, mock_permissions_service, mock_templates_response
    ):
        subject_template_filename = "subject_modify.html"

        mock_permissions_service.get_all_permissions_ui.return_value = ["any-value"]
        mock_permissions_service.get_user_permissions_ui.return_value = [
            "any-value",
            "any-other-value",
        ]

        response = self.client.get(
            "/subject/a1b2c3d4/modify", cookies={"rat": "user_token"}
        )

        mock_templates_response.assert_called_once_with(
            name=subject_template_filename,
            context={
                "request": ANY,
                "subject_name": "a1b2c3d4",
                "permissions": ["any-value"],
                "user_permissions": ["any-value", "any-other-value"],
            },
        )

        assert response.status_code == 200


class TestCreateSubjectPage(BaseClientTest):
    @patch.object(Jinja2Templates, "TemplateResponse")
    @patch("api.controller_ui.subject_management.permissions_service")
    def test_calls_templating_engine_with_expected_arguments(
        self, mock_permissions_service, mock_templates_response
    ):
        subject_template_filename = "subject_create.html"
        expected_permission_object = {"any-key": "any-value"}

        mock_permissions_service.get_all_permissions_ui.return_value = (
            expected_permission_object
        )

        response = self.client.get("/subject/create", cookies={"rat": "user_token"})

        mock_templates_response.assert_called_once_with(
            name=subject_template_filename,
            context={"request": ANY, "permissions": expected_permission_object},
        )

        mock_permissions_service.get_all_permissions_ui.assert_called_once()

        assert response.status_code == 200
