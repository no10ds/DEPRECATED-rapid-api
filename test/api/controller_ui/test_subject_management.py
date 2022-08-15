from unittest.mock import patch, ANY

from fastapi.templating import Jinja2Templates

from api.application.services.permissions_service import PermissionsService
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
    @patch("api.controller_ui.subject_management.subject_service")
    @patch("api.controller_ui.subject_management.permissions_service")
    def test_calls_templating_engine_with_expected_arguments(
        self, mock_permissions_service, mock_subject_service, mock_templates_response
    ):
        subject_template_filename = "subject_modify.html"

        mock_permissions_service.get_all_permissions_ui.return_value = ["any-value"]
        mock_permissions_service.get_user_permissions_ui.return_value = [
            "any-other-value"
        ]
        mock_subject_service.get_subject_name_by_id.return_value = "the_subject_name"

        response = self.client.get(
            "/subject/a1b2c3d4/modify", cookies={"rat": "user_token"}
        )

        mock_subject_service.get_subject_name_by_id.assert_called_once_with("a1b2c3d4")
        mock_templates_response.assert_called_once_with(
            name=subject_template_filename,
            context={
                "request": ANY,
                "subject_id": "a1b2c3d4",
                "subject_name": "the_subject_name",
                "permissions": ["any-value"],
                "subject_permissions": ["any-other-value"],
                "error_message": None,
            },
        )

        assert response.status_code == 200

    @patch.object(Jinja2Templates, "TemplateResponse")
    @patch("api.controller_ui.subject_management.permissions_service")
    def test_calls_templating_engine_with_error_message_when_subject_permissions_not_found(
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
                "subject_id": "a1b2c3d4",
                "subject_name": None,
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
                "subject_id": "a1b2c3d4",
                "subject_name": None,
                "permissions": [],
                "subject_permissions": [],
                "error_message": "Something went wrong. Please contact your system administrator",
            },
        )

        assert response.status_code == 200

    @patch.object(Jinja2Templates, "TemplateResponse")
    @patch("api.controller_ui.subject_management.subject_service")
    @patch("api.controller_ui.subject_management.permissions_service")
    def test_calls_templating_engine_with_error_message_when_subject_name_not_found(
        self, mock_permissions_service, mock_subject_service, mock_templates_response
    ):
        subject_template_filename = "subject_modify.html"

        mock_permissions_service.get_all_permissions_ui.return_value = ["any-value"]
        mock_permissions_service.get_user_permissions_ui.return_value = [
            "any-other-value"
        ]
        mock_subject_service.get_subject_name_by_id.side_effect = UserError(
            "The subject name could not be found"
        )

        response = self.client.get(
            "/subject/a1b2c3d4/modify", cookies={"rat": "user_token"}
        )

        mock_templates_response.assert_called_once_with(
            name=subject_template_filename,
            context={
                "request": ANY,
                "subject_id": "a1b2c3d4",
                "subject_name": None,
                "permissions": [],
                "subject_permissions": [],
                "error_message": "Error: The subject name could not be found. Please go back and try again.",
            },
        )

        assert response.status_code == 200


class TestModifySubjectSuccessPage(BaseClientTest):
    @patch.object(PermissionsService, "get_user_permissions_ui")
    @patch.object(Jinja2Templates, "TemplateResponse")
    @patch("api.controller_ui.subject_management.subject_service")
    def test_calls_templating_engine_with_expected_arguments(
        self,
        mock_subject_service,
        mock_templates_response,
        mock_get_user_ui_permissions,
    ):
        subject_template_filename = "success.html"

        mock_subject_service.get_subject_name_by_id.return_value = "the_subject_name"
        mock_get_user_ui_permissions.return_value = {
            "PROTECTED_WRITE": [
                {
                    "display_name_full": "Write protected test",
                }
            ],
            "GLOBAL_READ": [
                {
                    "display_name_full": "Read private",
                }
            ],
        }

        response = self.client.get(
            "/subject/a1b2c3d4/modify/success", cookies={"rat": "user_token"}
        )

        mock_subject_service.get_subject_name_by_id.assert_called_once_with("a1b2c3d4")
        mock_templates_response.assert_called_once_with(
            name=subject_template_filename,
            context={
                "request": ANY,
                "subject_id": "a1b2c3d4",
                "subject_name": "the_subject_name",
                "subject_permissions_display_names": [
                    "Write protected test",
                    "Read private",
                ],
                "error_message": "",
            },
        )

        assert response.status_code == 200

    @patch.object(Jinja2Templates, "TemplateResponse")
    @patch("api.controller_ui.subject_management.subject_service")
    def test_calls_templating_engine_with_expected_arguments_when_getting_subject_name_fails(
        self, mock_subject_service, mock_templates_response
    ):
        subject_template_filename = "success.html"

        mock_subject_service.get_subject_name_by_id.side_effect = AWSServiceError("")

        response = self.client.get(
            "/subject/a1b2c3d4/modify/success", cookies={"rat": "user_token"}
        )

        mock_templates_response.assert_called_once_with(
            name=subject_template_filename,
            context={
                "request": ANY,
                "subject_id": "a1b2c3d4",
                "subject_name": None,
                "subject_permissions_display_names": [],
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
