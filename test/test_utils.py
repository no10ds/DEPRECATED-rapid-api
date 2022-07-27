import json
from io import StringIO

from botocore.response import StreamingBody

from api.common.config.constants import CONTENT_ENCODING


def set_encoded_content(string_content: str) -> bytes:
    return bytes(string_content.encode(CONTENT_ENCODING))


def mock_schema_response():
    body_json = {
        "metadata": {
            "domain": "test_domain",
            "dataset": "test_dataset",
            "sensitivity": "PUBLIC",
        },
        "columns": [
            {
                "name": "colname1",
                "partition_index": 0,
                "data_type": "Int64",
                "allow_null": True,
            }
        ],
    }

    body_encoded = json.dumps(body_json)
    response_body = StreamingBody(StringIO(body_encoded), len(body_encoded))

    return {"Body": response_body}


def mock_list_schemas_response(
    domain: str = "test_domain",
    dataset: str = "test_dataset",
    sensitivity: str = "PUBLIC",
):
    return {
        "Contents": [
            {"Key": "data/schemas/"},
            {"Key": f"data/schemas/{sensitivity}/"},
            {
                "Key": f"data/schemas/{sensitivity}/{domain}-{dataset}.json",
            },
        ],
        "Name": "bucket-name",
        "Prefix": "data/schemas",
        "EncodingType": "url",
    }


def mock_secure_dataset_endpoint():
    """Naming is very important with dependency overrides; unfortunately we cannot just return a Mock object"""

    def secure_dataset_endpoint():
        pass

    return secure_dataset_endpoint


def mock_secure_endpoint():
    """Naming is very important with dependency overrides; unfortunately we cannot just return a Mock object"""

    def secure_endpoint():
        pass

    return secure_endpoint
