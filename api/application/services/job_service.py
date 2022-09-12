from typing import Dict

from api.adapter.dynamodb_adapter import DynamoDBAdapter
from api.common.logger import AppLogger
from api.domain.Jobs.Job import JobStep, Job, JobStatus
from api.domain.Jobs.UploadJob import UploadJob


class JobService:
    def __init__(self, db_adapter=DynamoDBAdapter()):
        self.db_adapter = db_adapter

    def get_all_jobs(self) -> list[Dict]:
        return self.db_adapter.get_jobs()

    def create_upload_job(self, filename: str) -> UploadJob:
        job = UploadJob(filename)
        self.db_adapter.store_upload_job(job)
        return job

    def update_step(self, job: Job, step: JobStep) -> None:
        AppLogger.info(f"Setting step for job {job.job_id} to {step.value}")
        job.set_step(step)
        self.db_adapter.update_job(job)

    def succeed(self, job: Job) -> None:
        AppLogger.info(f"Job {job.job_id} has succeeded")
        job.set_status(JobStatus.SUCCESS)
        self.db_adapter.update_job(job)
