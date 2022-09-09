from api.adapter.dynamodb_adapter import DynamoDBAdapter
from api.domain.Jobs.UploadJob import UploadJob

ad = DynamoDBAdapter()

job = UploadJob("crislaude.csv")

ad.store_upload_job(job)
