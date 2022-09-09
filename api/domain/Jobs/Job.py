import uuid
from typing import Set

from api.common.utilities import BaseEnum


class JobStatus(BaseEnum):
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    IN_PROGRESS = "IN PROGRESS"


class JobType(BaseEnum):
    UPLOAD = "UPLOAD"


class JobStep(BaseEnum):
    pass


def generate_uuid() -> str:
    return str(uuid.uuid4())


class Job:
    def __init__(self, job_type: JobType, step: JobStep):
        self.step: JobStep = step
        self.job_type: JobType = job_type
        self.status: JobStatus = JobStatus.IN_PROGRESS
        self.job_id: str = generate_uuid()
        self.errors: Set[str] = set()

    def set_step(self, step: JobStep) -> None:
        self.step = step

    def set_status(self, status: JobStatus):
        self.status = status

    def set_errors(self, errors: Set[str]):
        self.errors = errors
