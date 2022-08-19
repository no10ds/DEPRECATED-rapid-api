from unittest.mock import patch, ANY, Mock

from fastapi.templating import Jinja2Templates

from api.application.services.data_service import DataService
from api.application.services.dataset_service import DatasetService
from api.common.config.auth import Action
from api.domain.enriched_schema import (
    EnrichedSchema,
    EnrichedSchemaMetadata,
    EnrichedColumn,
)
from api.domain.schema_metadata import Owner
from test.api.common.controller_test_utils import BaseClientTest


class TestUploadPage(BaseClientTest):
    @patch("api.controller_ui.data_management.parse_token")
    @patch.object(DatasetService, "get_authorised_datasets")
    @patch.object(Jinja2Templates, "TemplateResponse")
    def test_calls_templating_engine_with_expected_arguments(
        self, mock_templates_response, mock_get_authorised_datasets, mock_parse_token
    ):
        upload_template_filename = "upload.html"
        datasets = ["domain1/dataset1.csv", "domain2/dataset2.csv"]
        subject_id = "subject_id"

        mock_token = Mock()
        mock_token.subject = subject_id
        mock_parse_token.return_value = mock_token
        mock_get_authorised_datasets.return_value = datasets

        expected_datasets = {
            "domain1": ["dataset1.csv"],
            "domain2": ["dataset2.csv"],
        }

        response = self.client.get("/upload", cookies={"rat": "user_token"})

        mock_parse_token.assert_called_once_with("user_token")
        mock_get_authorised_datasets.assert_called_once_with(subject_id, Action.WRITE)
        mock_templates_response.assert_called_once_with(
            name=upload_template_filename,
            context={
                "request": ANY,
                "datasets": expected_datasets,
            },
        )

        assert response.status_code == 200


class TestSelectDatasetPage(BaseClientTest):
    @patch("api.controller_ui.data_management.parse_token")
    @patch.object(DatasetService, "get_authorised_datasets")
    @patch.object(Jinja2Templates, "TemplateResponse")
    def test_calls_templating_engine_with_expected_arguments(
        self, mock_templates_response, mock_get_authorised_datasets, mock_parse_token
    ):
        subject_id = "123abc"
        download_template_filename = "datasets.html"

        mock_token = Mock()
        mock_token.subject = subject_id
        mock_parse_token.return_value = mock_token

        mock_get_authorised_datasets.return_value = [
            "domain1/dataset1",
            "domain1/dataset2",
            "domain2/dataset3",
        ]

        expected_datasets = {
            "domain1": ["dataset1", "dataset2"],
            "domain2": ["dataset3"],
        }

        response = self.client.get("/download", cookies={"rat": "user_token"})

        mock_get_authorised_datasets.assert_called_once_with(subject_id, Action.READ)
        mock_templates_response.assert_called_once_with(
            name=download_template_filename,
            context={"request": ANY, "datasets": expected_datasets},
        )

        assert response.status_code == 200


class TestDownloadPage(BaseClientTest):
    @patch.object(Jinja2Templates, "TemplateResponse")
    @patch.object(DataService, "get_dataset_info")
    def test_calls_templating_engine_with_expected_arguments(
        self, mock_get_dataset_info, mock_templates_response
    ):
        expected_response = EnrichedSchema(
            metadata=EnrichedSchemaMetadata(
                domain="some",
                dataset="other",
                sensitivity="PUBLIC",
                owners=[Owner(name="owner", email="owner@email.com")],
                number_of_rows=48718,
                number_of_columns=3,
                last_updated="2022-03-01 11:03:49+00:00",
            ),
            columns=[
                EnrichedColumn(
                    name="colname1",
                    partition_index=0,
                    data_type="Int64",
                    allow_null=True,
                ),
                EnrichedColumn(
                    name="colname2",
                    partition_index=None,
                    data_type="object",
                    allow_null=False,
                ),
                EnrichedColumn(
                    name="colname3",
                    partition_index=None,
                    data_type="date",
                    allow_null=False,
                    format="%d/%m/%Y",
                    statistics={"max": "2021-07-01", "min": "2014-01-01"},
                ),
            ],
        )

        expected_info = {
            "domain": "domain1",
            "dataset": "dataset1",
            "number_of_rows": 48718,
            "number_of_columns": 3,
            "last_updated": "1 Mar 2022 at 11:03:49",
            "columns": [
                {
                    "name": "colname1",
                    "data_type": "Int64",
                    "allow_null": True,
                    "max": "-",
                    "min": "-",
                },
                {
                    "name": "colname2",
                    "data_type": "object",
                    "allow_null": False,
                    "max": "-",
                    "min": "-",
                },
                {
                    "name": "colname3",
                    "data_type": "date",
                    "allow_null": False,
                    "max": "2021-07-01",
                    "min": "2014-01-01",
                },
            ],
        }
        mock_get_dataset_info.return_value = expected_response

        download_template_filename = "download.html"

        response = self.client.get(
            "/download/domain1/dataset1", cookies={"rat": "user_token"}
        )

        mock_templates_response.assert_called_once_with(
            name=download_template_filename,
            context={"request": ANY, "dataset_info": expected_info},
        )

        mock_get_dataset_info.assert_called_once_with("domain1", "dataset1")

        assert response.status_code == 200
