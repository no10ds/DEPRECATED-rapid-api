from unittest.mock import patch

from api.application.services.job_service import JobService
from test.api.common.controller_test_utils import BaseClientTest


class TestListJob(BaseClientTest):
    @patch.object(JobService, "get_all_jobs")
    def test_returns_list_of_all_currently_tracked_jobs(self, mock_get_all_jobs):
        expected_response = [
            {
                "job_id": "abc-123",
                "job_type": "UPLOAD",
                "status": "IN PROGRESS",
                "step": "VALIDATION",
                "errors": [],
                "filename": "UPLOAD",
            },
            {
                "job_id": "def-456",
                "job_type": "UPLOAD",
                "status": "FAILED",
                "step": "VALIDATION",
                "errors": ["Error: Invalid column name"],
                "filename": "UPLOAD",
            },
        ]

        mock_get_all_jobs.return_value = expected_response

        response = self.client.get(
            "/jobs", headers={"Authorization": "Bearer test-token"}
        )

        mock_get_all_jobs.assert_called_once()

        assert response.status_code == 200
        assert response.json() == expected_response


class TestGetJob(BaseClientTest):
    @patch.object(JobService, "get_job")
    def test_returns_single_currently_tracked_job(self, mock_get_job):
        expected_response = {
            "job_id": "abc-123",
            "job_type": "UPLOAD",
            "status": "IN PROGRESS",
            "step": "VALIDATION",
            "errors": [],
            "filename": "UPLOAD",
        }

        mock_get_job.return_value = expected_response

        response = self.client.get(
            "/jobs/abc-123", headers={"Authorization": "Bearer test-token"}
        )

        mock_get_job.assert_called_once_with("abc-123")

        assert response.status_code == 200
        assert response.json() == expected_response
