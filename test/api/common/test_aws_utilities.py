from unittest.mock import patch, MagicMock

from api.common.aws_utilities import get_available_ip_count
from api.common.config.aws import AWS_REGION


@patch("api.common.aws_utilities.boto3")
def test_get_available_ip_count(mock_boto3):
    count = 2
    subnet = "subnet-123"

    mock_subnet = MagicMock()
    mock_subnet.available_ip_address_count = count

    mock_ec2 = MagicMock()
    mock_ec2.Subnet.return_value = mock_subnet

    mock_boto3.resource.return_value = mock_ec2

    res = get_available_ip_count(subnet)

    assert res == count
    mock_boto3.resource.assert_called_once_with("ec2", region_name=AWS_REGION)
    mock_ec2.Subnet.assert_called_once_with(subnet)
