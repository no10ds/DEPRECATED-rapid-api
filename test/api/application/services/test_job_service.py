from unittest.mock import patch

from api.adapter.dynamodb_adapter import DynamoDBAdapter
from api.application.services.job_service import JobService


class TestListAllJobs:
    def setup(self):
        self.job_service = JobService()

    @patch.object(DynamoDBAdapter, "get_jobs")
    def test_get_all_jobs(self, mock_get_jobs):
        # GIVEN
        expected = [
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
                "job_id": "def-456",
                "status": "FAILED",
                "step": "VALIDATION",
                "errors": ["Error: Unknown data type"],
                "filename": "filename2.csv",
            },
        ]

        mock_get_jobs.return_value = expected

        # WHEN
        result = self.job_service.get_all_jobs()

        # THEN
        assert result == expected
        mock_get_jobs.assert_called_once()
