from botocore.exceptions import ClientError
from unittest.mock import Mock
import pytest

from api.adapter.ssm_adapter import SSMAdapter
from api.common.custom_exceptions import AWSServiceError


class TestSSMAdapter:
    def setup_method(self):
        self.ssm_boto_client = Mock()
        self.ssm_adapter = SSMAdapter(self.ssm_boto_client)

    def test_get_parameter_success(self):
        mock_response = {
            "Parameter": {
                "Name": "name",
                "Type": "String",
                "Value": "value",
            },
            "ResponseMetadata": {"HTTPStatusCode": 200},
        }
        self.ssm_boto_client.get_parameter = Mock(return_value=mock_response)
        response = self.ssm_adapter.get_parameter("name")
        self.ssm_boto_client.get_parameter.assert_called_once_with(Name="name")
        assert response == "value"

    def test_get_parameter_fails(self):
        self.ssm_boto_client.get_parameter = Mock(
            side_effect=ClientError(
                error_response={"Error": {"Code": "SomeException"}},
                operation_name="GetParameter",
            )
        )
        with pytest.raises(
            AWSServiceError,
            match="There was an unexpected error when retrieving the parameter 'name'",
        ):
            self.ssm_adapter.get_parameter("name")

    def test_put_parameter_success(self):
        mock_response = {
            "Version": 123,
            "Tier": "Standard",
            "ResponseMetadata": {"HTTPStatusCode": 200},
        }
        self.ssm_boto_client.put_parameter = Mock(return_value=mock_response)
        self.ssm_adapter.put_parameter("name", "value")
        self.ssm_boto_client.put_parameter.assert_called_once_with(
            Name="name", Value="value", Overwrite=True
        )

    def test_put_parameter_fails(self):
        self.ssm_boto_client.put_parameter = Mock(
            side_effect=ClientError(
                error_response={"Error": {"Code": "SomeException"}},
                operation_name="PutParameter",
            )
        )
        with pytest.raises(
            AWSServiceError,
            match="There was an unexpected error when pushing the value'value' to the parameter 'name'",
        ):
            self.ssm_adapter.put_parameter("name", "value")
