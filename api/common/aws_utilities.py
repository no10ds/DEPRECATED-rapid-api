import base64
import json
from typing import Dict

import boto3
from botocore.exceptions import ClientError

from api.common.config.aws import AWS_REGION


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


def get_available_ip_count(subnet_id: str) -> int:
    ec2 = boto3.resource("ec2", region_name=AWS_REGION)
    subnet = ec2.Subnet(subnet_id)
    return subnet.available_ip_address_count
