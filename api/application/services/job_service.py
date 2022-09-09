from typing import Dict

from api.adapter.dynamodb_adapter import DynamoDBAdapter
from api.common.logger import AppLogger
from api.domain.Jobs.UploadJob import UploadStep, UploadJob


class JobService:
    def __init__(self, db_adapter=DynamoDBAdapter()):
        self.db_adapter = db_adapter

    def get_all_jobs(self) -> list[Dict]:
        return self.db_adapter.get_jobs()

    def create_upload_job(self, filename: str) -> UploadJob:
        job = UploadJob(filename)
        self.db_adapter.store_upload_job(job)
        return job

    def update_step(self, job: UploadJob, step: UploadStep) -> None:
        AppLogger.info(f"Setting step for job {job.job_id} to {step.value}")
        job.set_step(step)
        self.db_adapter.update_job(job)
