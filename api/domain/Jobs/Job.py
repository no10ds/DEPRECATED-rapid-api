import uuid
from typing import List

from api.common.utilities import BaseEnum


class JobStatus(BaseEnum):
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    IN_PROGRESS = "IN_PROGRESS"


class JobType(BaseEnum):
    UPLOAD = "UPLOAD"


def generate_uuid() -> str:
    return str(uuid.uuid4())


class Job:
    def __init__(self, job_type: JobType):
        self.job_type: JobType = job_type
        self.status: JobStatus = JobStatus.IN_PROGRESS
        self.job_id: str = generate_uuid()
        self.errors: List[str] = list()
