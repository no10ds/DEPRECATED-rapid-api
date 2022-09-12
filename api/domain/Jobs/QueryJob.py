import time
from typing import Optional

from api.common.config.constants import QUERY_JOB_EXPIRY_DAYS
from api.domain.Jobs.Job import Job, JobType, JobStep


class QueryStep(JobStep):
    INITIALISATION = "INITIALISATION"
    QUERY = "QUERY"


class QueryJob(Job):
    def __init__(self, domain: str, dataset: str):
        super().__init__(JobType.QUERY, QueryStep.INITIALISATION)
        self.domain: str = domain
        self.dataset: str = dataset
        self.results_url: Optional[str] = None
        self.expiry_time: int = int(time.time() + QUERY_JOB_EXPIRY_DAYS * 24 * 60 * 60)