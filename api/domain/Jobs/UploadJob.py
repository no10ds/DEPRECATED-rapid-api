import time

from api.common.config.constants import UPLOAD_JOB_EXPIRY_DAYS
from api.domain.Jobs.Job import Job, JobType, JobStep


class UploadStep(JobStep):
    INITIALISATION = "INITIALISATION"
    VALIDATION = "VALIDATION"
    RAW_DATA_UPLOAD = "RAW_DATA_UPLOAD"
    DATA_UPLOAD = "DATA_UPLOAD"
    CLEAN_UP = "CLEAN_UP"
    NONE = "-"


class UploadJob(Job):
    def __init__(
        self,
        subject_id: str,
        job_id: str,
        filename: str,
        raw_file_identifier: str,
        domain: str,
        dataset: str,
        version: int,
    ):
        super().__init__(JobType.UPLOAD, UploadStep.INITIALISATION, subject_id, job_id)
        self.filename: str = filename
        self.raw_file_identifier: str = raw_file_identifier
        self.domain: str = domain
        self.dataset: str = dataset
        self.version: int = version
        self.expiry_time: int = int(time.time() + UPLOAD_JOB_EXPIRY_DAYS * 24 * 60 * 60)
