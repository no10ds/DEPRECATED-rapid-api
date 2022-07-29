import json
from abc import ABC
from http import HTTPStatus

import boto3
import requests
from requests.auth import HTTPBasicAuth

from api.common.config.aws import DATA_BUCKET, DOMAIN_NAME
from api.common.config.constants import CONTENT_ENCODING
from test.e2e.e2e_test_utils import get_secret, AuthenticationFailedError


class BaseJourneyTest(ABC):
    base_url = f"https://{DOMAIN_NAME}"
    datasets_endpoint = f"{base_url}/datasets"

    e2e_test_domain = "test_e2e"

    schemas_directory = "data/schemas/PUBLIC"
    data_directory = f"data/{e2e_test_domain}"
    raw_data_directory = f"raw_data/{e2e_test_domain}"

    filename = "test_journey_file.csv"

    def upload_dataset_url(self, domain: str, dataset: str) -> str:
        return f"{self.datasets_endpoint}/{domain}/{dataset}"

    def query_dataset_url(self, domain: str, dataset: str) -> str:
        return f"{self.datasets_endpoint}/{domain}/{dataset}/query"

    def info_dataset_url(self, domain: str, dataset: str) -> str:
        return f"{self.datasets_endpoint}/{domain}/{dataset}/info"

    def list_dataset_files_url(self, domain: str, dataset: str) -> str:
        return f"{self.datasets_endpoint}/{domain}/{dataset}/files"

    def delete_data_url(self, domain: str, dataset: str, filename: str) -> str:
        return f"{self.datasets_endpoint}/{domain}/{dataset}/{filename}"

    def status_url(self) -> str:
        return f"{self.base_url}/status"


class TestGeneralBehaviour(BaseJourneyTest):
    def test_http_request_is_redirected_to_https(self):
        response = requests.get(self.status_url())
        assert f"https://{DOMAIN_NAME}" in response.url

    def test_status_always_accessible(self):
        api_url = self.status_url()
        response = requests.get(api_url)
        assert response.status_code == HTTPStatus.OK


class TestUnauthenticatedJourneys(BaseJourneyTest):
    def test_query_is_forbidden_when_no_token_provided(self):
        url = self.query_dataset_url("mydomain", "unknowndataset")
        response = requests.post(url)
        assert response.status_code == HTTPStatus.FORBIDDEN

    def test_upload_is_forbidden_when_no_token_provided(self):
        files = {"file": (self.filename, open("./test/e2e/" + self.filename, "rb"))}
        url = self.upload_dataset_url(self.e2e_test_domain, "upload")
        response = requests.post(url, files=files)
        assert response.status_code == HTTPStatus.FORBIDDEN

    def test_list_is_forbidden_when_no_token_provided(self):
        response = requests.post(self.datasets_endpoint)
        assert response.status_code == HTTPStatus.FORBIDDEN


class TestUnauthorisedJourney(BaseJourneyTest):
    def setup_class(self):
        token_url = f"https://{DOMAIN_NAME}/oauth2/token"

        credentials = get_secret(
            secret_name="E2E_TEST_COGNITO_APP_CLIENT_ID_AND_SECRET"  # pragma: allowlist secret
        )
        cognito_client_id = credentials["CLIENT_ID"]
        cognito_client_secret = credentials["CLIENT_SECRET"]  # pragma: allowlist secret

        auth = HTTPBasicAuth(cognito_client_id, cognito_client_secret)

        headers = {"Content-Type": "application/x-www-form-urlencoded"}

        payload = {
            "grant_type": "client_credentials",
            "client_id": cognito_client_id,
            "scope": f"https://{DOMAIN_NAME}/WRITE_ALL",
        }

        response = requests.post(token_url, auth=auth, headers=headers, json=payload)

        if response.status_code != HTTPStatus.OK:
            raise AuthenticationFailedError(f"{response.status_code}")

        self.token = json.loads(response.content.decode(CONTENT_ENCODING))[
            "access_token"
        ]

    # Utils -------------

    def generate_auth_headers(self):
        return {"Authorization": f"Bearer {self.token}"}

    # Tests -------------

    def test_query_existing_dataset_when_not_authorised_to_read(self):
        url = self.query_dataset_url(self.e2e_test_domain, "query")
        response = requests.post(url, headers=self.generate_auth_headers())
        assert response.status_code == HTTPStatus.UNAUTHORIZED

    def test_existing_dataset_info_when_not_authorised_to_read(self):
        url = self.info_dataset_url(self.e2e_test_domain, "query")
        response = requests.get(url, headers=self.generate_auth_headers())
        assert response.status_code == HTTPStatus.UNAUTHORIZED


class TestAuthenticatedJourneys(BaseJourneyTest):
    s3_client = boto3.client("s3")

    def setup_class(self):
        token_url = f"https://{DOMAIN_NAME}/oauth2/token"

        credentials = get_secret(
            secret_name="E2E_TEST_COGNITO_APP_CLIENT_ID_AND_SECRET"  # pragma: allowlist secret
        )

        cognito_client_id = credentials["CLIENT_ID"]

        cognito_client_secret = credentials["CLIENT_SECRET"]  # pragma: allowlist secret

        auth = HTTPBasicAuth(cognito_client_id, cognito_client_secret)

        headers = {"Content-Type": "application/x-www-form-urlencoded"}

        payload = {
            "grant_type": "client_credentials",
            "client_id": cognito_client_id,
            "scope": f"https://{DOMAIN_NAME}/READ_ALL https://{DOMAIN_NAME}/WRITE_ALL",
        }

        response = requests.post(token_url, auth=auth, headers=headers, json=payload)

        if response.status_code != HTTPStatus.OK:
            raise AuthenticationFailedError(f"{response.status_code}")

        self.token = json.loads(response.content.decode(CONTENT_ENCODING))[
            "access_token"
        ]

    # Utils -------------

    def generate_auth_headers(self):
        return {"Authorization": f"Bearer {self.token}"}

    def delete_all_data_and_raw_files_for_(self, domain: str, filetype: str = ".csv"):
        files = self.s3_client.list_objects_v2(
            Bucket=DATA_BUCKET, Prefix=f"{self.raw_data_directory}/{domain}"
        )

        files_to_delete = [
            file["Key"].rsplit("/", 1)[-1]
            for file in files["Contents"]
            if file["Key"].endswith(filetype)
        ]

        filepaths_to_delete = []

        for filename in files_to_delete:
            filepaths_to_delete.append(
                {"Key": f"{self.raw_data_directory}/{domain}/{filename}"}
            )
            filepaths_to_delete.append(
                {"Key": f"{self.data_directory}/{domain}/{filename}"}
            )

        self.s3_client.delete_objects(
            Bucket=DATA_BUCKET, Delete={"Objects": filepaths_to_delete}
        )

    def upload_test_file_to_(self, data_directory: str, domain: str):
        self.s3_client.put_object(
            Bucket=DATA_BUCKET,
            Key=f"{data_directory}/{domain}/{self.filename}",
            Body=open("./test/e2e/" + self.filename, "rb"),
        )

    # Tests -------------

    def test_list_when_authorised(self):
        response = requests.post(
            self.datasets_endpoint,
            headers=self.generate_auth_headers(),
            json={"tags": {"test": "e2e"}},
        )
        assert response.status_code == HTTPStatus.OK

    def test_uploads_when_authorised(self):
        files = {"file": (self.filename, open("./test/e2e/" + self.filename, "rb"))}
        url = self.upload_dataset_url(self.e2e_test_domain, "upload")
        response = requests.post(url, headers=self.generate_auth_headers(), files=files)

        assert response.status_code == HTTPStatus.CREATED

        self.delete_all_data_and_raw_files_for_(domain="upload", filetype=".csv")

    def test_gets_existing_dataset_info_when_authorised(self):
        url = self.info_dataset_url(domain=self.e2e_test_domain, dataset="query")
        response = requests.get(url, headers=(self.generate_auth_headers()))
        assert response.status_code == HTTPStatus.OK

    def test_queries_non_existing_dataset_when_authorised(self):
        url = self.query_dataset_url("mydomain", "unknowndataset")
        response = requests.post(url, headers=self.generate_auth_headers())
        assert response.status_code == HTTPStatus.BAD_REQUEST

    def test_queries_existing_dataset_as_json_when_authorised(self):
        url = self.query_dataset_url(domain=self.e2e_test_domain, dataset="query")
        response = requests.post(url, headers=(self.generate_auth_headers()))
        assert response.status_code == HTTPStatus.OK

    def test_queries_existing_dataset_as_csv_when_authorised(self):
        url = self.query_dataset_url(domain=self.e2e_test_domain, dataset="query")
        headers = {
            "Accept": "text/csv",
            "Authorization": "Bearer " + self.token,
        }
        response = requests.post(url, headers=headers)
        assert response.status_code == HTTPStatus.OK
        assert (
            response.text
            == '"","year","month","destination","arrival","type","status"\n'
            + '0,"2017","7","Leeds","London","regular","completed"\n'
            + '1,"2017","7","Darlington","Durham","regular","completed"\n'
        )

    def test_fails_to_query_when_authorised_and_sql_injection_attempted(self):
        url = self.query_dataset_url(domain=self.e2e_test_domain, dataset="query")
        body = {"filter": "';DROP TABLE test_e2e--"}
        response = requests.post(url, headers=(self.generate_auth_headers()), json=body)
        assert response.status_code == HTTPStatus.FORBIDDEN

    def test_deletes_existing_data_when_authorised(self):
        # Upload files directly to relevant directories in S3
        self.upload_test_file_to_(self.raw_data_directory, domain="delete")
        self.upload_test_file_to_(self.data_directory, domain="delete")

        # Get available datasets
        url = self.list_dataset_files_url(domain=self.e2e_test_domain, dataset="delete")
        available_datasets_response = requests.get(
            url, headers=(self.generate_auth_headers())
        )
        assert available_datasets_response.status_code == HTTPStatus.OK

        response_list = json.loads(available_datasets_response.text)
        assert self.filename in response_list

        # Delete chosen dataset
        first_dataset_file = response_list[0]
        url2 = self.delete_data_url(
            domain=self.e2e_test_domain, dataset="delete", filename=first_dataset_file
        )
        response2 = requests.delete(url2, headers=(self.generate_auth_headers()))

        assert response2.status_code == HTTPStatus.NO_CONTENT
