import json
from abc import ABC
from http import HTTPStatus
from typing import List

import boto3
import requests
from requests.auth import HTTPBasicAuth

from api.common.config.aws import DATA_BUCKET, DOMAIN_NAME
from api.common.config.constants import CONTENT_ENCODING
from test.e2e.e2e_test_utils import get_secret, AuthenticationFailedError
from test.scripts.delete_protected_domain_permission import (
    delete_protected_domain_permission_from_db,
)


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

    def create_protected_domain_url(self, domain: str) -> str:
        return f"{self.base_url}/protected_domains/{domain}"

    def list_protected_domain_url(self) -> str:
        return f"{self.base_url}/protected_domains"

    def modify_client_permissions_url(self) -> str:
        return f"{self.base_url}/client/permissions"

    def delete_data_url(self, domain: str, dataset: str, filename: str) -> str:
        return f"{self.datasets_endpoint}/{domain}/{dataset}/{filename}"

    def permissions_url(self) -> str:
        return f"{self.base_url}/permissions"

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

    def test_list_permissions_is_forbidden_when_no_token_provided(self):
        response = requests.get(self.permissions_url())
        assert response.status_code == HTTPStatus.FORBIDDEN


class TestUnauthorisedJourney(BaseJourneyTest):
    def setup_class(self):
        token_url = f"https://{DOMAIN_NAME}/oauth2/token"

        write_all_credentials = get_secret(secret_name="E2E_TEST_CLIENT_WRITE_ALL")

        cognito_client_id = write_all_credentials["CLIENT_ID"]
        cognito_client_secret = write_all_credentials[
            "CLIENT_SECRET"
        ]  # pragma: allowlist secret

        auth = HTTPBasicAuth(cognito_client_id, cognito_client_secret)

        headers = {"Content-Type": "application/x-www-form-urlencoded"}

        payload = {"grant_type": "client_credentials", "client_id": cognito_client_id}

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

    def test_list_permissions_when_not_user_admin(self):
        response = requests.get(
            self.permissions_url(), headers=self.generate_auth_headers()
        )
        assert response.status_code == HTTPStatus.UNAUTHORIZED


class TestAuthenticatedDataJourneys(BaseJourneyTest):
    s3_client = boto3.client("s3")

    def setup_class(self):
        token_url = f"https://{DOMAIN_NAME}/oauth2/token"

        read_and_write_credentials = get_secret(
            secret_name="E2E_TEST_CLIENT_READ_ALL_WRITE_ALL"  # pragma: allowlist secret
        )

        cognito_client_id = read_and_write_credentials["CLIENT_ID"]
        cognito_client_secret = read_and_write_credentials[
            "CLIENT_SECRET"
        ]  # pragma: allowlist secret

        auth = HTTPBasicAuth(cognito_client_id, cognito_client_secret)

        headers = {"Content-Type": "application/x-www-form-urlencoded"}

        payload = {
            "grant_type": "client_credentials",
            "client_id": cognito_client_id,
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


class TestAuthenticatedUserJourneys(BaseJourneyTest):
    s3_client = boto3.client("s3")
    cognito_client_id = None

    def setup_class(self):
        token_url = f"https://{DOMAIN_NAME}/oauth2/token"

        read_and_write_credentials = get_secret(
            secret_name="E2E_TEST_CLIENT_USER_ADMIN"  # pragma: allowlist secret
        )

        self.cognito_client_id = read_and_write_credentials["CLIENT_ID"]
        cognito_client_secret = read_and_write_credentials[
            "CLIENT_SECRET"
        ]  # pragma: allowlist secret

        auth = HTTPBasicAuth(self.cognito_client_id, cognito_client_secret)

        headers = {"Content-Type": "application/x-www-form-urlencoded"}

        payload = {
            "grant_type": "client_credentials",
            "client_id": self.cognito_client_id,
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

    def test_lists_all_permissions_contains_all_default_permissions(self):
        response = requests.get(
            self.permissions_url(), headers=self.generate_auth_headers()
        )

        expected_permissions = [
            "READ_ALL",
            "WRITE_ALL",
            "READ_PUBLIC",
            "WRITE_PUBLIC",
            "READ_PRIVATE",
            "WRITE_PRIVATE",
            "DATA_ADMIN",
            "USER_ADMIN",
        ]

        response_json = response.json()

        assert response.status_code == HTTPStatus.OK
        assert all((permission in response_json for permission in expected_permissions))

    def test_lists_subject_permissions(self):
        response = requests.get(
            f"{self.permissions_url()}/{self.cognito_client_id}",
            headers=self.generate_auth_headers(),
        )

        assert response.status_code == HTTPStatus.OK
        assert response.json() == ["USER_ADMIN"]


class TestAuthenticatedProtectedDomainJourneys(BaseJourneyTest):
    s3_client = boto3.client("s3")
    cognito_client_id = None

    def setup_class(self):
        token_url = f"https://{DOMAIN_NAME}/oauth2/token"

        read_and_write_credentials = get_secret(
            secret_name="E2E_TEST_CLIENT_DATA_ADMIN"  # pragma: allowlist secret
        )

        self.cognito_client_id = read_and_write_credentials["CLIENT_ID"]
        cognito_client_secret = read_and_write_credentials[
            "CLIENT_SECRET"
        ]  # pragma: allowlist secret

        auth = HTTPBasicAuth(self.cognito_client_id, cognito_client_secret)

        headers = {"Content-Type": "application/x-www-form-urlencoded"}

        payload = {
            "grant_type": "client_credentials",
            "client_id": self.cognito_client_id,
        }

        response = requests.post(token_url, auth=auth, headers=headers, json=payload)

        if response.status_code != HTTPStatus.OK:
            raise AuthenticationFailedError(f"{response.status_code}")

        self.token = json.loads(response.content.decode(CONTENT_ENCODING))[
            "access_token"
        ]

    @classmethod
    def teardown_class(cls):
        delete_protected_domain_permission_from_db("test_e2e")

    # Utils -------------
    def generate_auth_headers(self):
        return {"Authorization": f"Bearer {self.token}"}

    def assume_permissions(self, permissions: List[str]):
        modification_url = self.modify_client_permissions_url()
        payload = {
            "subject_id": self.cognito_client_id,
            "permissions": ["USER_ADMIN", "DATA_ADMIN", *permissions],
        }

        response = requests.put(
            modification_url, headers=self.generate_auth_headers(), json=payload
        )
        assert response.status_code == HTTPStatus.OK

    def reset_permissions(self):
        self.assume_permissions([])

    # Tests -------------
    def test_create_protected_domain(self):
        # Create protected domain
        create_url = self.create_protected_domain_url("test_e2e")
        response = requests.post(create_url, headers=self.generate_auth_headers())
        assert response.status_code == HTTPStatus.CREATED

        # Lists created protected domain
        list_url = self.list_protected_domain_url()
        response = requests.get(list_url, headers=self.generate_auth_headers())
        assert "test_e2e" in response.json()

        # Not authorised to access existing protected domain
        url = self.query_dataset_url(
            domain="test_e2e_protected", dataset="do_not_delete"
        )
        response = requests.post(url, headers=self.generate_auth_headers())
        assert response.status_code == HTTPStatus.UNAUTHORIZED

    def test_allows_access_to_protected_domain_when_granted_permission(self):
        self.assume_permissions(["READ_PROTECTED_TEST_E2E_PROTECTED"])

        url = self.query_dataset_url("test_e2e_protected", "do_not_delete")
        response = requests.post(url, headers=self.generate_auth_headers())

        assert response.status_code == HTTPStatus.OK
        assert response.json() == {
            "0": {
                "year": "2017",
                "month": "7",
                "destination": "Leeds",
                "arrival": "London",
                "type": "regular",
                "status": "completed",
            },
            "1": {
                "year": "2017",
                "month": "7",
                "destination": "Darlington",
                "arrival": "Durham",
                "type": "regular",
                "status": "completed",
            },
        }

        self.reset_permissions()
