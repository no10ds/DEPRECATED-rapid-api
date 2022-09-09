from api.domain.Jobs.Job import Job, JobType, JobStep


class UploadStep(JobStep):
    VALIDATION = "VALIDATION"


class UploadJob(Job):
    def __init__(self, filename: str):
        super().__init__(JobType.UPLOAD, UploadStep.VALIDATION)
        self.filename: str = filename
