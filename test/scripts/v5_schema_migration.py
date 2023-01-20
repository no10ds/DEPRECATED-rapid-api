"""
Removes the whitespace and add a description field to the schemas
"""

import os
import boto3
import dotenv
import json

dotenv.load_dotenv()

AWS_REGION = os.environ["AWS_REGION"]
DATA_BUCKET = os.environ["DATA_BUCKET"]

s3_client = boto3.client("s3")
glue_client = boto3.client("glue", region_name=AWS_REGION)


objects = s3_client.list_objects_v2(
    Bucket=DATA_BUCKET,
    Prefix="data/schemas",
)["Contents"]

for _object in objects:
    res = s3_client.get_object(Bucket=DATA_BUCKET, Key=_object["Key"])
    json_object = json.loads(res["Body"].read().decode("utf-8"))
    json_object |= {"description": ""}
    s3_client.put_object(
        Body=json.dumps(json_object), Bucket=DATA_BUCKET, Key=_object["Key"]
    )
