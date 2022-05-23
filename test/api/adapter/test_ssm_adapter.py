from unittest.mock import Mock

from api.adapter.ssm_adapter import SSMAdapter


class TestSSMAdapter:
    def setup_method(self):
        self.ssm_boto_client = Mock()
        self.ssm_adapter = SSMAdapter(self.ssm_boto_client)

    def test_get_parameter(self):

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

    def test_put_parameter(self):
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
