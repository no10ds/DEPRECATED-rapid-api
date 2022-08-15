from unittest.mock import patch

from api.common.custom_exceptions import AWSServiceError
from test.api.common.controller_test_utils import BaseClientTest


class TestListSubjects(BaseClientTest):
    @patch("api.controller.subjects.subject_service")
    def test_returns_list_of_all_subjects(self, mock_subject_service):
        expected = [
            {"key1": "value1", "key2": "value2"},
            {"key1": "value1", "key2": "value2"},
        ]

        mock_subject_service.list_subjects.return_value = expected

        response = self.client.get(
            "/subjects", headers={"Authorization": "Bearer test-token"}
        )

        mock_subject_service.list_subjects.assert_called_once()

        assert response.status_code == 200
        assert response.json() == expected

    @patch("api.controller.subjects.subject_service")
    def test_returns_server_error_when_failure_in_aws(self, mock_subject_service):
        mock_subject_service.list_subjects.side_effect = AWSServiceError("The message")

        response = self.client.get(
            "/subjects", headers={"Authorization": "Bearer test-token"}
        )

        mock_subject_service.list_subjects.assert_called_once()

        assert response.status_code == 500
        assert response.json() == {"details": "The message"}
