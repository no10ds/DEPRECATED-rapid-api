from unittest.mock import patch

from api.domain.Jobs.Job import JobType, JobStatus
from api.domain.Jobs.UploadJob import UploadJob, UploadStep


@patch("api.domain.Jobs.Job.uuid")
def test_initialise_upload_job(mock_uuid):
    mock_uuid.uuid4.return_value = "abc-123"

    job = UploadJob("some-filename.csv")

    assert job.job_id == "abc-123"
    assert job.job_type == JobType.UPLOAD
    assert job.status == JobStatus.IN_PROGRESS
    assert job.step == UploadStep.VALIDATION
    assert job.errors == set()
    assert job.filename == "some-filename.csv"
