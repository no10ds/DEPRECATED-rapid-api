from unittest.mock import patch, ANY, Mock

from fastapi.templating import Jinja2Templates

from api.application.services.dataset_service import DatasetService
from test.api.common.controller_test_utils import BaseClientTest


class TestUploadPage(BaseClientTest):
    @patch("api.controller_ui.data_management.parse_token")
    @patch.object(DatasetService, "get_authorised_datasets")
    @patch.object(Jinja2Templates, "TemplateResponse")
    def test_calls_templating_engine_with_expected_arguments(
        self, mock_templates_response, mock_upload_service, mock_parse_token
    ):
        upload_template_filename = "upload.html"
        datasets = ["dataset.csv", "dataset2.csv"]
        subject_id = "subject_id"

        mock_token = Mock()
        mock_token.subject = subject_id
        mock_parse_token.return_value = mock_token
        mock_upload_service.return_value = datasets

        response = self.client.get("/upload", cookies={"rat": "user_token"})

        mock_parse_token.assert_called_once_with("user_token")
        mock_upload_service.assert_called_once_with(subject_id)
        mock_templates_response.assert_called_once_with(
            name=upload_template_filename,
            context={
                "request": ANY,
                "datasets": datasets,
            },
        )

        assert response.status_code == 200


class TestDownloadPage(BaseClientTest):
    @patch.object(Jinja2Templates, "TemplateResponse")
    def test_calls_templating_engine_with_expected_arguments(
        self, mock_templates_response
    ):
        download_template_filename = "datasets.html"

        expected_datasets = None

        response = self.client.get("/download", cookies={"rat": "user_token"})

        mock_templates_response.assert_called_once_with(
            name=download_template_filename,
            context={"request": ANY, "datasets": expected_datasets},
        )

        assert response.status_code == 200
