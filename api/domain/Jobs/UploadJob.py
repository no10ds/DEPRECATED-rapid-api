import time

from api.common.config.constants import UPLOAD_JOB_EXPIRY_DAYS
from api.domain.Jobs.Job import Job, JobType, JobStep


class UploadStep(JobStep):
    INITIALISATION = "INITIALISATION"
    VALIDATION = "VALIDATION"
    RAW_DATA_UPLOAD = "RAW_DATA_UPLOAD"
    DATA_UPLOAD = "DATA_UPLOAD"
    CLEAN_UP = "CLEAN_UP"


class UploadJob(Job):
    def __init__(self, filename: str):
        super().__init__(JobType.UPLOAD, UploadStep.INITIALISATION)
        self.filename: str = filename
        self.expiry_time: int = int(time.time() + UPLOAD_JOB_EXPIRY_DAYS * 24 * 60 * 60)
