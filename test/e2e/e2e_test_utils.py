import base64
import json
from pydoc import cli
from typing import Dict

import boto3
from botocore.exceptions import ClientError

from api.common.config.aws import AWS_REGION


class AuthenticationFailedError(Exception):
    pass
print(AWS_REGION)

def get_secret(secret_name: str) -> Dict:
    print(secret_name)
    client = boto3.client(service_name="secretsmanager", region_name=AWS_REGION)
    print(client.get_secret_value(SecretId=secret_name))
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
