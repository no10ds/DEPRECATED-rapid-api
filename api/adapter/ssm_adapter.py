import boto3
from botocore.exceptions import ClientError

from api.common.config.aws import AWS_REGION
from api.common.custom_exceptions import AWSServiceError
from api.common.logger import AppLogger


class SSMAdapter:
    def __init__(self, ssm_client=boto3.client("ssm", region_name=AWS_REGION)):
        self._ssm_client = ssm_client

    def get_parameter(self, name: str) -> str:
        try:
            response = self._ssm_client.get_parameter(Name=name)
        except ClientError as error:
            AppLogger.info(f"Unable to retrieve ssm parameter {error}")
            raise AWSServiceError(
                f"There was an unexpected error when retrieving the parameter '{name}'"
            )
        return response["Parameter"]["Value"]

    def put_parameter(self, name: str, value: str):
        try:
            self._ssm_client.put_parameter(Name=name, Value=value, Overwrite=True)
        except ClientError as error:
            AppLogger.info(f"Unable to store ssm parameter {error}")
            raise AWSServiceError(
                f"There was an unexpected error when pushing the value'{value}' to the parameter '{name}'"
            )
