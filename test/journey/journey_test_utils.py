import base64
import json
from typing import Dict

import boto3
from botocore.exceptions import ClientError

from api.common.config.aws import AWS_REGION


class AuthenticationFailedError(Exception):
    pass


def get_secret(secret_name: str) -> Dict:
    client = boto3.client(service_name="secretsmanager", region_name=AWS_REGION)
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as error:
        raise error
    else:
        if "SecretString" in get_secret_value_response:
            secret = get_secret_value_response["SecretString"]
        else:
            secret = base64.b64decode(get_secret_value_response["SecretBinary"])

    return json.loads(secret)


def cleanup_query_files(params):
    session = boto3.Session()
    s3 = session.resource("s3")
    my_bucket = s3.Bucket(params["query_bucket"])
    for item in my_bucket.objects.filter(Prefix=params["query_path"]):
        item.delete()


# Deletes all files in your path so use carefully!
def cleanup_data_files(params):
    session = boto3.Session()
    s3 = session.resource("s3")
    my_bucket = s3.Bucket(params["data_bucket"])
    for item in my_bucket.objects.filter(Prefix=params["data_path"]):
        item.delete()


# Deletes all files in your path so use carefully!
def cleanup_raw_files(params):
    session = boto3.Session()
    s3 = session.resource("s3")
    my_bucket = s3.Bucket(params["data_bucket"])
    for item in my_bucket.objects.filter(Prefix=params["raw_data_path"]):
        item.delete()


def get_file_names(params):
    session = boto3.Session()
    s3 = session.resource("s3")
    my_bucket = s3.Bucket(params["data_bucket"])
    for item in my_bucket.objects.filter(Prefix="raw_data/playwright/playwright01/"):
        if item.key.endswith("test_journey_file.csv"):
            file_name = item.key.split("/")[-1]
            return file_name
        else:
            raise Exception("No file found")
    print("Cleaning up...")
    cleanup_query_files(params)
    cleanup_data_files(params)
    cleanup_raw_files(params)


def athena_query(params):
    client = boto3.client("athena")
    response = client.start_query_execution(
        QueryString="DROP TABLE `playwright_playwright01`;",
        QueryExecutionContext={
            "Catalog": params["catalog"],
            "Database": params["database"],
        },
        ResultConfiguration={
            "OutputLocation": "s3://"
            + params["query_bucket"]
            + "/"
            + params["query_path"],
            "EncryptionConfiguration": {
                "EncryptionOption": "SSE_KMS",
                "KmsKey": "arn:aws:kms:"
                + params["region"]
                + ":"
                + params["account_id"]
                + ":key/"
                + params["kmskey"],
            },
            "ExpectedBucketOwner": params["account_id"],
            "AclConfiguration": {"S3AclOption": "BUCKET_OWNER_FULL_CONTROL"},
        },
    )
    return response