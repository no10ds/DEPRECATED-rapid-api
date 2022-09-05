from unittest.mock import patch

from api.adapter.aws_resource_adapter import AWSResourceAdapter
from api.common.utilities import handle_version_retrieval


class TestHandleVersionRetrieval:
    @patch.object(AWSResourceAdapter, "get_version_from_crawler_tags")
    def test_retrieve_version_from_crawler_when_version_is_none(
        self, mock_get_version_from_crawler_tags
    ):

        expected_version = 3

        mock_get_version_from_crawler_tags.return_value = expected_version

        actual_version = handle_version_retrieval("domain1", "dataset1", None)

        mock_get_version_from_crawler_tags.assert_called_once_with(
            "domain1", "dataset1"
        )

        assert actual_version == expected_version

    def test_return_user_input_version_when_version_is_defined(self):

        expected_version = 4

        actual_version = handle_version_retrieval("domain1", "dataset1", 4)

        assert actual_version == expected_version
