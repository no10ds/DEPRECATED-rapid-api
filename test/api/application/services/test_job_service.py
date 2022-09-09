from unittest.mock import patch

from api.adapter.dynamodb_adapter import DynamoDBAdapter
from api.application.services.job_service import JobService
from api.domain.Jobs.Job import JobStatus
from api.domain.Jobs.UploadJob import UploadStep, UploadJob


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


class TestCreateUploadJob:
    def setup(self):
        self.job_service = JobService()

    @patch("api.domain.Jobs.Job.uuid")
    @patch.object(DynamoDBAdapter, "store_upload_job")
    def test_creates_upload_job(self, mock_store_upload_job, mock_uuid):
        # GIVEN
        mock_uuid.uuid4.return_value = "abc-123"

        # WHEN
        result = self.job_service.create_upload_job("file1.csv")

        # THEN
        assert result.job_id == "abc-123"
        assert result.filename == "file1.csv"
        assert result.step == UploadStep.INITIALISATION
        assert result.status == JobStatus.IN_PROGRESS
        mock_store_upload_job.assert_called_once_with(result)


class TestUpdateJob:
    def setup(self):
        self.job_service = JobService()

    @patch("api.domain.Jobs.Job.uuid")
    @patch.object(DynamoDBAdapter, "update_job")
    def test_get_all_jobs(self, mock_update_job, mock_uuid):
        # GIVEN
        mock_uuid.uuid4.return_value = "abc-123"
        job = UploadJob("file1.csv")

        assert job.step == UploadStep.INITIALISATION

        # WHEN
        self.job_service.update_step(job, UploadStep.CLEAN_UP)

        # THEN
        assert job.step == UploadStep.CLEAN_UP
        mock_update_job.assert_called_once_with(job)
