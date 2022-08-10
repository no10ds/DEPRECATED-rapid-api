from test.api.common.controller_test_utils import BaseClientTest


class TestStatus(BaseClientTest):
    def test_http_status_response_is_200_status(self):
        response = self.client.get("/status")
        assert response.status_code == 200
