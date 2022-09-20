from unittest.mock import patch, ANY

from fastapi.templating import Jinja2Templates

from api.application.services.permissions_service import PermissionsService
from api.application.services.subject_service import SubjectService
from api.common.custom_exceptions import UserError, AWSServiceError
from test.api.common.controller_test_utils import BaseClientTest


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
            },
        )

        assert response.status_code == 200

    @patch("api.controller_ui.subject_management.permissions_service")
    def test_returns_error_when_subject_does_not_exist(self, mock_permissions_service):
        mock_permissions_service.get_user_permissions_ui.side_effect = UserError(
            "Subject does not exist"
        )

        response = self.client.get(
            "/subject/a1b2c3d4/modify", cookies={"rat": "user_token"}
        )

        assert response.status_code == 400
        assert response.json() == {"details": "Subject does not exist"}

    @patch("api.controller_ui.subject_management.permissions_service")
    def test_returns_error_when_aws_error(self, mock_permissions_service):
        mock_permissions_service.get_all_permissions_ui.side_effect = AWSServiceError(
            "Something went wrong"
        )

        response = self.client.get(
            "/subject/a1b2c3d4/modify", cookies={"rat": "user_token"}
        )

        assert response.status_code == 500
        assert response.json() == {"details": "Something went wrong"}

    @patch("api.controller_ui.subject_management.subject_service")
    @patch("api.controller_ui.subject_management.permissions_service")
    def test_returns_error_when_subject_name_not_found(
        self, mock_permissions_service, mock_subject_service
    ):
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

        assert response.status_code == 400
        assert response.json() == {"details": "The subject name could not be found"}


class TestSubjectPage(BaseClientTest):
    @patch.object(SubjectService, "list_subjects")
    @patch.object(Jinja2Templates, "TemplateResponse")
    def test_calls_templating_engine_with_expected_arguments(
        self, mock_templates_response, mock_list_subjects
    ):
        subject_template_filename = "subject.html"

        expected_subjects = {
            "Client Apps": [
                {"subject_id": "subject_1", "subject_name": "A_subject_1_name"},
                {"subject_id": "subject_2", "subject_name": "B_subject_2_name"},
            ],
            "Users": [
                {"subject_id": "subject_3", "subject_name": "C_subject_3_name"},
                {"subject_id": "subject_4", "subject_name": "D_subject_4_name"},
            ],
        }

        mock_list_subjects.return_value = [
            {
                "subject_id": "subject_2",
                "subject_name": "B_subject_2_name",
                "type": "CLIENT",
            },
            {
                "subject_id": "subject_1",
                "subject_name": "A_subject_1_name",
                "type": "CLIENT",
            },
            {
                "subject_id": "subject_4",
                "subject_name": "D_subject_4_name",
                "type": "USER",
            },
            {
                "subject_id": "subject_3",
                "subject_name": "C_subject_3_name",
                "type": "USER",
            },
        ]

        response = self.client.get("/subject", cookies={"rat": "user_token"})

        mock_list_subjects.assert_called_once()

        mock_templates_response.assert_called_once_with(
            name=subject_template_filename,
            context={"request": ANY, "subjects": expected_subjects},
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
            },
        )

        assert response.status_code == 200

    @patch("api.controller_ui.subject_management.subject_service")
    def test_returns_error_when_getting_subject_name_fails(self, mock_subject_service):
        mock_subject_service.get_subject_name_by_id.side_effect = AWSServiceError(
            "Retrieving subject name failed"
        )

        response = self.client.get(
            "/subject/a1b2c3d4/modify/success", cookies={"rat": "user_token"}
        )

        assert response.status_code == 500
        assert response.json() == {"details": "Retrieving subject name failed"}


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
            headers={"Cache-Control": "no-store"},
        )

        mock_permissions_service.get_all_permissions_ui.assert_called_once()

        assert response.status_code == 200
