from unittest.mock import patch

from api.domain.Jobs.Job import JobType, JobStatus
from api.domain.Jobs.UploadJob import UploadJob, UploadStep


@patch("api.domain.Jobs.Job.uuid")
@patch("api.domain.Jobs.UploadJob.time")
def test_initialise_upload_job(mock_time, mock_uuid):
    mock_time.time.return_value = 1000
    mock_uuid.uuid4.return_value = "abc-123"

    job = UploadJob("some-filename.csv")

    assert job.job_id == "abc-123"
    assert job.job_type == JobType.UPLOAD
    assert job.status == JobStatus.IN_PROGRESS
    assert job.step == UploadStep.INITIALISATION
    assert job.errors == set()
    assert job.filename == "some-filename.csv"
    assert job.expiry_time == 605800