from unittest.mock import patch, MagicMock

from api.common.custom_exceptions import AWSServiceError
from api.common.utilities import build_error_message_list, construct_dataset_metadata
from api.domain.dataset_metadata import DatasetMetadata


class TestConstructDataasetMetadata:
    def setup_method(self):
        self.dataset_metadata = DatasetMetadata

    @patch("api.common.utilities.aws_resource_adapter")
    @patch.object(DatasetMetadata, "set_version")
    def test_construct_dataset_metadata(
        self, mock_set_version: MagicMock, mock_aws_resource_adapter
    ):
        expected = DatasetMetadata("layer", "domain", "dataset", 1)
        res = construct_dataset_metadata("layer", "domain", "dataset", 1)
        assert res == expected
        mock_set_version.assert_called_once_with(mock_aws_resource_adapter)


class TestBuildErrorList:
    def test_build_list_when_message_is_list(self):
        result = build_error_message_list(AWSServiceError(["error1", "error2"]))

        assert result == ["error1", "error2"]

    def test_build_list_when_message_is_string(self):
        result = build_error_message_list(AWSServiceError("error1"))

        assert result == ["error1"]

    def test_build_list_when_exception_has_no_message(self):
        result = build_error_message_list(ValueError("error1"))

        assert result == ["error1"]
