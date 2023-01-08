from unittest.mock import patch, ANY, Mock

from fastapi.templating import Jinja2Templates

from api.application.services.job_service import JobService
from api.common.config.constants import BASE_API_PATH
from test.api.common.controller_test_utils import BaseClientTest


class TestJobsPage(BaseClientTest):
    @patch("api.controller_ui.task_management.parse_token")
    @patch.object(JobService, "get_all_jobs")
    @patch.object(Jinja2Templates, "TemplateResponse")
    def test_calls_templating_engine_with_expected_arguments(
        self, mock_templates_response, mock_get_all_jobs, mock_parse_token
    ):
        upload_template_filename = "jobs.html"
        subject_id = "subject_id"

        jobs = [
            {
                "type": "UPLOAD",
                "job_id": "abc-123",
                "status": "IN PROGRESS",
                "step": "VALIDATION",
                "errors": None,
                "filename": "filename1.csv",
            },
            {
                "type": "UPLOAD",
                "job_id": "abc-123",
                "status": "IN PROGRESS",
                "step": "RAW_DATA_UPLOAD",
                "errors": None,
                "filename": "filename1.csv",
            },
        ]

        mock_token = Mock()
        mock_token.subject = subject_id
        mock_parse_token.return_value = mock_token
        mock_get_all_jobs.return_value = jobs

        expected_jobs = [
            {
                "type": "UPLOAD",
                "job_id": "abc-123",
                "status": "IN PROGRESS",
                "step": "Validation",
                "errors": None,
                "filename": "filename1.csv",
            },
            {
                "type": "UPLOAD",
                "job_id": "abc-123",
                "status": "IN PROGRESS",
                "step": "Raw data upload",
                "errors": None,
                "filename": "filename1.csv",
            },
        ]

        response = self.client.get(f"{BASE_API_PATH}/tasks", cookies={"rat": "user_token"})

        mock_parse_token.assert_called_once_with("user_token")
        mock_get_all_jobs.assert_called_once_with(subject_id)
        mock_templates_response.assert_called_once_with(
            name=upload_template_filename,
            context={
                "request": ANY,
                "jobs": expected_jobs,
            },
        )

        assert response.status_code == 200


class TestJobDetailsPage(BaseClientTest):
    @patch.object(JobService, "get_job")
    @patch.object(Jinja2Templates, "TemplateResponse")
    def test_calls_templating_engine_with_expected_arguments(
        self, mock_templates_response, mock_get_job
    ):
        upload_template_filename = "job_detail.html"

        job = {
            "type": "UPLOAD",
            "job_id": "abc-123",
            "status": "IN PROGRESS",
            "step": "VALIDATION",
            "errors": None,
            "filename": "filename1.csv",
        }

        mock_get_job.return_value = job

        expected_job = {
            "type": "UPLOAD",
            "job_id": "abc-123",
            "status": "IN PROGRESS",
            "step": "Validation",
            "errors": None,
            "filename": "filename1.csv",
        }

        response = self.client.get(f"{BASE_API_PATH}/tasks/123-456-789", cookies={"rat": "user_token"})

        mock_get_job.assert_called_once_with("123-456-789")
        mock_templates_response.assert_called_once_with(
            name=upload_template_filename,
            context={
                "request": ANY,
                "job": expected_job,
            },
        )

        assert response.status_code == 200
