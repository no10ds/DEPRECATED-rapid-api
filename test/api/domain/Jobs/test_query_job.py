from unittest.mock import patch

from api.domain.Jobs.Job import JobType, JobStatus
from api.domain.Jobs.QueryJob import QueryJob, QueryStep


@patch("api.domain.Jobs.Job.uuid")
@patch("api.domain.Jobs.QueryJob.time")
def test_initialise_upload_job(mock_time, mock_uuid):
    mock_time.time.return_value = 1000
    mock_uuid.uuid4.return_value = "abc-123"

    job = QueryJob("111222333")

    assert job.job_id == "abc-123"
    assert job.job_type == JobType.QUERY
    assert job.status == JobStatus.IN_PROGRESS
    assert job.step == QueryStep.INITIALISATION
    assert job.errors == set()
    assert job.subject_id == "111222333"
    assert job.results_url is None
    assert job.expiry_time == 87400
