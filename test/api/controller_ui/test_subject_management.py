from unittest.mock import patch, ANY

from fastapi.templating import Jinja2Templates

from test.api.common.controller_test_utils import BaseClientTest


class TestSubjectPage(BaseClientTest):
    @patch.object(Jinja2Templates, "TemplateResponse")
    def test_calls_templating_engine_with_expected_arguments(
        self, mock_templates_response
    ):
        subject_template_filename = "subject.html"

        response = self.client.get("/subject", cookies={"rat": "user_token"})

        mock_templates_response.assert_called_once_with(
            name=subject_template_filename,
            context={"request": ANY},
        )

        assert response.status_code == 200
