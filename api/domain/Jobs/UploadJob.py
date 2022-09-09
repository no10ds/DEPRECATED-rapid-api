from api.domain.Jobs.Job import Job, JobType


class UploadJob(Job):
    def __init__(self, filename: str):
        super().__init__(JobType.UPLOAD)
        self.filename: str = filename
