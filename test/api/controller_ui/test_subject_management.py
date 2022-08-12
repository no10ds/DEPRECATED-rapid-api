from unittest.mock import patch, ANY

from fastapi.templating import Jinja2Templates

from api.common.custom_exceptions import UserError, AWSServiceError
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
                "subject_permissions": ["any-value", "any-other-value"],
                "error_message": None,
            },
        )

        assert response.status_code == 200

    @patch.object(Jinja2Templates, "TemplateResponse")
    @patch("api.controller_ui.subject_management.permissions_service")
    def test_calls_templating_engine_with_error_message_when_subject_not_found(
        self, mock_permissions_service, mock_templates_response
    ):
        subject_template_filename = "subject_modify.html"

        mock_permissions_service.get_user_permissions_ui.side_effect = UserError(
            "Subject does not exist"
        )

        response = self.client.get(
            "/subject/a1b2c3d4/modify", cookies={"rat": "user_token"}
        )

        mock_templates_response.assert_called_once_with(
            name=subject_template_filename,
            context={
                "request": ANY,
                "subject_name": "a1b2c3d4",
                "permissions": [],
                "subject_permissions": [],
                "error_message": "Error: Subject does not exist. Please go back and try again.",
            },
        )

        assert response.status_code == 200

    @patch.object(Jinja2Templates, "TemplateResponse")
    @patch("api.controller_ui.subject_management.permissions_service")
    def test_calls_templating_engine_with_error_message_when_aws_error(
        self, mock_permissions_service, mock_templates_response
    ):
        subject_template_filename = "subject_modify.html"

        mock_permissions_service.get_all_permissions_ui.side_effect = AWSServiceError(
            "Something went wrong"
        )

        response = self.client.get(
            "/subject/a1b2c3d4/modify", cookies={"rat": "user_token"}
        )

        mock_templates_response.assert_called_once_with(
            name=subject_template_filename,
            context={
                "request": ANY,
                "subject_name": "a1b2c3d4",
                "permissions": [],
                "subject_permissions": [],
                "error_message": "Something went wrong. Please contact your system administrator",
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
