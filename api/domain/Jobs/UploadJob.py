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
