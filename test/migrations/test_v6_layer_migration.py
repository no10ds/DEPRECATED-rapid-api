import boto3
from moto import mock_s3, mock_glue
import pytest


AWS_REGION = "eu-west-2"
DATA_BUCKET = "the-bucket"
RESOURCE_PREFIX = "rapid"


@pytest.fixture
def s3_client():
    with mock_s3():
        conn = boto3.client("s3", regoin_name=AWS_REGION)
        yield conn


@pytest.mark.focus
@mock_s3
@mock_glue
class TestV6LayerMigrations:
    def setup_method(self, mock_glue, mock_s3):
        self.s3_client = boto3.client("s3")
        self.s3_client.create_bucket(
            Bucket=DATA_BUCKET,
            CreateBucketConfiguration={"LocationConstraint": AWS_REGION},
        )
        # self.resource_client = boto3.client("resourcegroupstaggingapi")

        # monkeypatch.setenv()

    def test_move_files_by_prefix(self):
        self.s3_client.list_objects_v2(Bucket=DATA_BUCKET)
