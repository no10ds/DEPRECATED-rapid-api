import boto3

from api.adapter.base_aws_adapter import BaseAWSAdapter
from api.common.config.aws import AWS_REGION


class SSMAdapter(BaseAWSAdapter):
    def __init__(self, ssm_client=boto3.client("ssm", region_name=AWS_REGION)):
        self._ssm_client = ssm_client

    def get_parameter(self, name: str) -> str:
        response = self._ssm_client.get_parameter(Name=name)
        self.validate_response(response)
        return response["Parameter"]["Value"]

    def put_parameter(self, name: str, value: str):
        response = self._ssm_client.put_parameter(
            Name=name, Value=value, Overwrite=True
        )
        self.validate_response(response)
