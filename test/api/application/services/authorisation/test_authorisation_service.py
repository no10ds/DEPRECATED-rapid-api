from typing import List
from unittest.mock import patch, Mock

import pytest
from fastapi import HTTPException
from fastapi.security import SecurityScopes
from jwt.exceptions import InvalidTokenError

from api.application.services.authorisation.acceptable_permissions import (
    AcceptablePermissions,
)
from api.application.services.authorisation.authorisation_service import (
    match_client_app_permissions,
    match_user_permissions,
    extract_client_app_scopes,
    extract_user_groups,
    protect_dataset_endpoint,
    secure_dataset_endpoint,
    check_credentials_availability,
    check_permissions,
    retrieve_permissions,
)
from api.common.config.auth import SensitivityLevel
from api.common.config.aws import DOMAIN_NAME
from api.common.custom_exceptions import (
    AuthorisationError,
    SchemaNotFoundError,
    UserCredentialsUnavailableError,
    BaseAppException,
)
from api.domain.permission_item import PermissionItem
from api.domain.token import Token


class TestExtractingPermissions:
    @patch("jwt.decode")
    @patch("api.application.services.authorisation.token_utils.jwks_client")
    def test_extract_token_permissions_for_apps(self, mock_jwks_client, mock_decode):
        mock_signing_key = Mock()
        mock_signing_key.key = "secret"
        mock_jwks_client.get_signing_key_from_jwt.return_value = mock_signing_key

        mock_decode.return_value = {
            "scope": f"https://{DOMAIN_NAME}/SOME_SCOPE https://{DOMAIN_NAME}/ANOTHER_SCOPE"
        }

        token_scopes = extract_client_app_scopes(None)

        mock_jwks_client.get_signing_key_from_jwt.assert_called_once_with(None)
        mock_decode.assert_called_once_with(None, "secret", algorithms=["RS256"])
        assert token_scopes == ["SOME_SCOPE", "ANOTHER_SCOPE"]

    @patch(
        "api.application.services.authorisation.authorisation_service.get_validated_token_payload"
    )
    def test_extract_token_permissions_for_users(
        self, mock_get_validated_token_payload
    ):
        token = "test-token"

        mock_get_validated_token_payload.return_value = {
            "cognito:groups": ["READ/domain/dataset", "WRITE/domain/dataset"],
            "scope": "phone openid email",
        }

        token_scopes = extract_user_groups(token)

        assert token_scopes == ["READ/domain/dataset", "WRITE/domain/dataset"]

    def test_throws_error_when_invalid_client_app_token(self):
        token = "invalid-token"
        with pytest.raises(
            AuthorisationError,
            match="Not enough permissions or access token is missing/invalid",
        ):
            extract_client_app_scopes(token)

    def test_throws_error_when_invalid_user_token(self):
        token = "invalid-token"
        with pytest.raises(
            AuthorisationError,
            match="Not enough permissions or access token is missing/invalid",
        ):
            extract_user_groups(token)

    @patch(
        "api.application.services.authorisation.authorisation_service.get_validated_token_payload"
    )
    def test_handles_valid_client_app_token_with_invalid_payload(
        self, mock_get_validated_token_payload
    ):
        token = "invalid-token"

        mock_get_validated_token_payload.return_value = {
            "invalid": ["read/domain/dataset", "write/domain/dataset"]
        }

        with pytest.raises(
            AuthorisationError,
            match="Not enough permissions or access token is missing/invalid",
        ):
            extract_client_app_scopes(token)

    @patch(
        "api.application.services.authorisation.authorisation_service.get_validated_token_payload"
    )
    def test_handles_valid_user_token_with_invalid_payload(
        self, mock_get_validated_token_payload
    ):
        token = "invalid-token"

        mock_get_validated_token_payload.return_value = {
            "invalid": ["read/domain/dataset", "write/domain/dataset"]
        }

        with pytest.raises(
            AuthorisationError,
            match="Not enough permissions or access token is missing/invalid",
        ):
            extract_user_groups(token)


class TestSecureDatasetEndpoint:
    def test_raises_error_when_no_user_credentials_provided(self):
        client_token = None
        user_token = None
        browser_request = True

        with pytest.raises(UserCredentialsUnavailableError):
            secure_dataset_endpoint(
                security_scopes=SecurityScopes(scopes=["READ"]),
                browser_request=browser_request,
                client_token=client_token,
                user_token=user_token,
                domain="mydomain",
                dataset="mydataset",
            )

    def test_raises_forbidden_exception_when_invalid_token(self):
        user_token = None
        client_token = None
        browser_request = False

        with pytest.raises(HTTPException):
            secure_dataset_endpoint(
                security_scopes=SecurityScopes(scopes=["READ"]),
                browser_request=browser_request,
                client_token=client_token,
                user_token=user_token,
                domain="mydomain",
                dataset="mydataset",
            )

    @patch("api.application.services.authorisation.authorisation_service.parse_token")
    def test_raises_unauthorised_exception_when_invalid_token_provided(
        self, mock_parse_token
    ):
        client_token = "invalid-token"
        browser_request = False

        mock_parse_token.side_effect = InvalidTokenError()

        with pytest.raises(HTTPException):
            secure_dataset_endpoint(
                security_scopes=SecurityScopes(scopes=["READ"]),
                browser_request=browser_request,
                client_token=client_token,
                user_token=None,
                domain="mydomain",
                dataset="mydataset",
            )

    @patch(
        "api.application.services.authorisation.authorisation_service.check_permissions"
    )
    @patch("api.application.services.authorisation.authorisation_service.parse_token")
    def test_parses_token_and_checks_permissions_when_user_token_available(
        self, mock_parse_token, mock_check_permissions
    ):
        user_token = "user-token"
        token = Token({"sub": "the-user-id", "cognito:groups": ["group1", "group2"]})

        mock_parse_token.return_value = token

        secure_dataset_endpoint(
            security_scopes=SecurityScopes(scopes=["READ"]),
            browser_request=True,
            client_token=None,
            user_token=user_token,
            domain="mydomain",
            dataset="mydataset",
        )

        mock_parse_token.assert_called_once_with("user-token")
        mock_check_permissions.assert_called_once_with(
            token, ["READ"], "mydomain", "mydataset"
        )

    @patch("api.domain.token.COGNITO_RESOURCE_SERVER_ID", "https://example.com")
    @patch(
        "api.application.services.authorisation.authorisation_service.check_permissions"
    )
    @patch("api.application.services.authorisation.authorisation_service.parse_token")
    def test_parses_token_and_checks_permissions_when_client_token_available(
        self, mock_parse_token, mock_check_permissions
    ):
        client_token = "client-token"
        token = Token(
            {
                "sub": "the-user-id",
                "scope": "https://example.com/scope1 https://example.com/scope2",
            }
        )

        mock_parse_token.return_value = token

        secure_dataset_endpoint(
            security_scopes=SecurityScopes(scopes=["READ"]),
            browser_request=False,
            client_token=client_token,
            user_token=None,
            domain="mydomain",
            dataset=None,
        )

        mock_parse_token.assert_called_once_with("client-token")
        mock_check_permissions.assert_called_once_with(
            token, ["READ"], "mydomain", None
        )

    @patch(
        "api.application.services.authorisation.authorisation_service.match_user_permissions"
    )
    @patch(
        "api.application.services.authorisation.authorisation_service.match_client_app_permissions"
    )
    @patch("api.application.services.authorisation.authorisation_service.Token")
    def test_check_permission_for_user_token(
        self, mock_token, mock_match_client_app_permissions, mock_match_user_permissions
    ):
        endpoint_scopes = ["READ"]
        domain = "test-domain"
        dataset = "test-dataset"
        mock_token.is_user_token.return_value = True
        mock_token.is_client_token.return_value = False

        check_permissions(mock_token, endpoint_scopes, domain, dataset)

        mock_match_user_permissions.assert_called_once_with(
            mock_token, endpoint_scopes, domain, dataset
        )
        mock_match_client_app_permissions.assert_not_called()

    @patch(
        "api.application.services.authorisation.authorisation_service.match_user_permissions"
    )
    @patch(
        "api.application.services.authorisation.authorisation_service.match_client_app_permissions"
    )
    @patch("api.application.services.authorisation.authorisation_service.Token")
    def test_check_permission_for_client_token(
        self, mock_token, mock_match_client_app_permissions, mock_match_user_permissions
    ):
        endpoint_scopes = ["READ"]
        domain = "test-domain"
        dataset = "test-dataset"
        mock_token.is_user_token.return_value = False
        mock_token.is_client_token.return_value = True

        check_permissions(mock_token, endpoint_scopes, domain, dataset)

        mock_match_user_permissions.assert_not_called()
        mock_match_client_app_permissions.assert_called_once_with(
            mock_token, endpoint_scopes, domain, dataset
        )

    @patch(
        "api.application.services.authorisation.authorisation_service.match_user_permissions"
    )
    @patch(
        "api.application.services.authorisation.authorisation_service.match_client_app_permissions"
    )
    @patch("api.application.services.authorisation.authorisation_service.Token")
    def test_check_permission_for_client_token_throws_http_exception(
        self, mock_token, mock_match_client_app_permissions, mock_match_user_permissions
    ):
        endpoint_scopes = ["READ"]
        domain = "test-domain"
        dataset = "test-dataset"
        mock_token.is_user_token.return_value = False
        mock_token.is_client_token.return_value = True
        mock_match_client_app_permissions.side_effect = SchemaNotFoundError()

        with pytest.raises(HTTPException):
            check_permissions(mock_token, endpoint_scopes, domain, dataset)


class TestProtectEndpoint:
    @patch(
        "api.application.services.authorisation.authorisation_service.match_user_permissions"
    )
    @patch(
        "api.application.services.authorisation.authorisation_service.extract_user_groups"
    )
    def test_matches_user_permissions_when_user_token_provided_from_any_source(
        self, mock_extract_user_groups, mock_match_user_permissions
    ):
        user_token = "test-token"
        browser_request = False
        mock_extract_user_groups.return_value = [
            "READ/domain/dataset",
            "WRITE/domain/dataset",
        ]
        protect_dataset_endpoint(
            security_scopes=SecurityScopes(scopes=["READ"]),
            browser_request=browser_request,
            client_token=None,
            user_token=user_token,
            domain="mydomain",
            dataset="mydataset",
        )

        mock_extract_user_groups.assert_called_once_with(user_token)
        mock_match_user_permissions.assert_called_once_with(
            ["READ/domain/dataset", "WRITE/domain/dataset"],
            ["READ"],
            "mydomain",
            "mydataset",
        )

    @patch(
        "api.application.services.authorisation.authorisation_service.match_user_permissions"
    )
    def test_raises_error_when_browser_makes_request_and_no_user_token_provided(
        self, mock_match_user_permissions
    ):
        user_token = None
        browser_request = True
        with pytest.raises(UserCredentialsUnavailableError):
            protect_dataset_endpoint(
                security_scopes=SecurityScopes(scopes=["READ"]),
                browser_request=browser_request,
                client_token=None,
                user_token=user_token,
                domain="mydomain",
                dataset="mydataset",
            )

        assert not mock_match_user_permissions.called

    @patch(
        "api.application.services.authorisation.authorisation_service.extract_client_app_scopes"
    )
    @patch(
        "api.application.services.authorisation.authorisation_service.match_client_app_permissions"
    )
    def test_matches_client_permissions_when_client_token_provided_from_programmatic_client(
        self, mock_match_client_app_permissions, mock_extract_client_app_scopes
    ):
        browser_request = False
        client_token = ("test-client-token",)
        mock_extract_client_app_scopes.return_value = ["READ_PUBLIC", "WRITE_PUBLIC"]

        protect_dataset_endpoint(
            security_scopes=SecurityScopes(scopes=["READ"]),
            browser_request=browser_request,
            client_token=client_token,
            user_token=None,
            domain="mydomain",
            dataset="mydataset",
        )

        mock_extract_client_app_scopes.assert_called_once_with(client_token)
        mock_match_client_app_permissions.assert_called_once_with(
            ["READ_PUBLIC", "WRITE_PUBLIC"], ["READ"], "mydomain", "mydataset"
        )

    @patch(
        "api.application.services.authorisation.authorisation_service.extract_client_app_scopes"
    )
    @patch(
        "api.application.services.authorisation.authorisation_service.match_client_app_permissions"
    )
    def test_raises_exception_when_schema_not_found_for_dataset(
        self, mock_match_client_app_permissions, mock_extract_client_app_scopes
    ):
        browser_request = False
        client_token = "test-client-token"
        mock_extract_client_app_scopes.return_value = ["READ_PUBLIC", "WRITE_PUBLIC"]
        mock_match_client_app_permissions.side_effect = SchemaNotFoundError()

        with pytest.raises(HTTPException):
            protect_dataset_endpoint(
                security_scopes=SecurityScopes(scopes=["READ"]),
                browser_request=browser_request,
                client_token=client_token,
                user_token=None,
                domain="mydomain",
                dataset="mydataset",
            )

    def test_raises_exception_when_no_credentials_exist(self):
        browser_request = False

        with pytest.raises(HTTPException):
            protect_dataset_endpoint(
                security_scopes=SecurityScopes(scopes=["READ"]),
                browser_request=browser_request,
                client_token=None,
                user_token=None,
                domain="mydomain",
                dataset="mydataset",
            )

    def test_raises_unauthorised_exception_when_no_credentials_provided(self):
        client_token = None
        user_token = None
        browser_request = False

        with pytest.raises(HTTPException):
            protect_dataset_endpoint(
                security_scopes=SecurityScopes(scopes=["READ"]),
                browser_request=browser_request,
                client_token=client_token,
                user_token=user_token,
                domain="mydomain",
                dataset="mydataset",
            )


class TestCheckCredentialsAvailability:
    def test_succeeds_when_at_least_user_credential_type_available(self):
        try:
            check_credentials_availability(
                browser_request=True, user_token="user-token", client_token=None
            )
        except UserCredentialsUnavailableError:
            pytest.fail("Unexpected UserCredentialsUnavailableError raised")
        except HTTPException:
            pytest.fail("Unexpected HTTPException raised")

    def test_succeeds_when_at_least_client_credential_type_available(self):
        try:
            check_credentials_availability(
                browser_request=True, user_token=None, client_token="client-token"
            )
        except UserCredentialsUnavailableError:
            pytest.fail("Unexpected UserCredentialsUnavailableError raised")
        except HTTPException:
            pytest.fail("Unexpected HTTPException raised")

    def test_raises_user_credentials_error_when_is_browser_request_with_no_credentials(
        self,
    ):
        with pytest.raises(UserCredentialsUnavailableError):
            check_credentials_availability(
                browser_request=True, user_token=None, client_token=None
            )

    def test_raises_http_error_when_is_not_browser_request_with_no_credentials(self):
        with pytest.raises(HTTPException):
            check_credentials_availability(
                browser_request=False, user_token=None, client_token=None
            )


class TestRetrievePermissions:
    @patch("api.application.services.authorisation.authorisation_service.db_adapter")
    def test_gets_subject_permissions_from_database_when_they_exist(
        self, mock_db_adapter
    ):
        token_with_only_db_permissions = Token({"sub": "the-subject-id"})

        mock_db_adapter.get_permissions_for_subject.return_value = [
            PermissionItem("1", "ALL", "READ"),
            PermissionItem("1", "PUBLIC", "WRITE"),
        ]

        result = retrieve_permissions(token_with_only_db_permissions)

        assert result == [
            "READ_ALL",
            "WRITE_PUBLIC",
        ]

    @patch("api.domain.token.COGNITO_RESOURCE_SERVER_ID", "https://example.com")
    @patch("api.application.services.authorisation.authorisation_service.db_adapter")
    def test_gets_subject_permissions_from_token_when_none_in_the_database(
        self, mock_db_adapter
    ):
        token_with_no_db_permissions = Token(
            {
                "sub": "the-subject-id",
                "scope": "https://example.com/READ_PRIVATE https://example.com/DATA_ADMIN",
            }
        )

        mock_db_adapter.get_permissions_for_subject.return_value = []

        result = retrieve_permissions(token_with_no_db_permissions)

        assert result == [
            "READ_PRIVATE",
            "DATA_ADMIN",
        ]

    @patch("api.application.services.authorisation.authorisation_service.db_adapter")
    def test_return_empty_permissions_list_when_no_permissions_found(
        self, mock_db_adapter
    ):
        token_with_no_permissions = Token(
            {
                "sub": "the-subject-id",
                "scope": "",
            }
        )

        mock_db_adapter.get_permissions_for_subject.return_value = []

        result = retrieve_permissions(token_with_no_permissions)

        assert result == []


class TestAcceptedScopes:
    @pytest.mark.parametrize(
        "accepted_scopes, token_scopes",
        [
            # READ endpoint
            (
                AcceptablePermissions(
                    required=set(), optional={"READ_ALL", "READ_PUBLIC"}
                ),
                ["READ_PUBLIC"],
            ),
            # WRITE endpoint
            (
                AcceptablePermissions(required=set(), optional={"WRITE_PUBLIC"}),
                ["WRITE_PUBLIC"],
            ),
            # Standalone action endpoints
            (
                AcceptablePermissions(required={"USER_ADMIN"}, optional=set()),
                ["USER_ADMIN"],
            ),
            (
                AcceptablePermissions(required={"DATA_ADMIN"}, optional=set()),
                ["DATA_ADMIN"],
            ),
        ],
    )
    def test_scopes_satisfy_acceptable_scopes(
        self, accepted_scopes: AcceptablePermissions, token_scopes: List[str]
    ):
        assert accepted_scopes.satisfied_by(token_scopes) is True

    @pytest.mark.parametrize(
        "accepted_scopes, token_scopes",
        [
            # READ endpoint
            (
                AcceptablePermissions(
                    required=set(), optional={"READ_ALL", "READ_PUBLIC"}
                ),
                [],
            ),  # No token scopes
            # WRITE endpoint
            (
                AcceptablePermissions(required=set(), optional={"WRITE_PUBLIC"}),
                ["READ_PUBLIC", "READ_ALL"],
            ),
            # Standalone action endpoints
            (
                AcceptablePermissions(required={"USER_ADMIN"}, optional=set()),
                ["READ_ALL"],
            ),
            (
                AcceptablePermissions(required={"DATA_ADMIN"}, optional=set()),
                ["WRITE_ALL"],
            ),
        ],
    )
    def test_scopes_do_not_satisfy_acceptable_scopes(
        self, accepted_scopes: AcceptablePermissions, token_scopes: List[str]
    ):
        assert accepted_scopes.satisfied_by(token_scopes) is False


class TestAppPermissionsMatching:
    def setup_method(self):
        self.mock_s3_client = Mock()

    @patch("api.application.services.authorisation.authorisation_service.s3_adapter")
    @pytest.mark.parametrize(
        "domain, dataset, sensitivity, token_scopes, endpoint_scopes",
        [
            # No endpoint scopes allow anyone access
            ("domain", "dataset", SensitivityLevel.PUBLIC, ["READ_PUBLIC"], []),
            # Token with correct action and sensitivity privilege
            ("domain", "dataset", SensitivityLevel.PUBLIC, ["READ_PUBLIC"], ["READ"]),
            ("domain", "dataset", SensitivityLevel.PRIVATE, ["READ_PRIVATE"], ["READ"]),
            ("domain", "dataset", SensitivityLevel.PUBLIC, ["WRITE_PUBLIC"], ["WRITE"]),
            (
                "domain",
                "dataset",
                SensitivityLevel.PRIVATE,
                ["WRITE_PRIVATE"],
                ["WRITE"],
            ),
            # Token with ALL permission
            ("domain", "dataset", SensitivityLevel.PUBLIC, ["READ_ALL"], ["READ"]),
            ("domain", "dataset", SensitivityLevel.PRIVATE, ["READ_ALL"], ["READ"]),
            ("domain", "dataset", SensitivityLevel.PUBLIC, ["WRITE_ALL"], ["WRITE"]),
            ("domain", "dataset", SensitivityLevel.PRIVATE, ["WRITE_ALL"], ["WRITE"]),
            # Higher sensitivity levels imply lower ones
            ("domain", "dataset", SensitivityLevel.PUBLIC, ["READ_PRIVATE"], ["READ"]),
            (
                "domain",
                "dataset",
                SensitivityLevel.PUBLIC,
                ["WRITE_PRIVATE"],
                ["WRITE"],
            ),
            # Standalone scopes (no domain or dataset, different type of action)
            (None, None, None, ["USER_ADMIN"], ["USER_ADMIN"]),
            (None, None, None, ["DATA_ADMIN"], ["DATA_ADMIN"]),
        ],
    )
    def test_matches_permissions(
        self,
        mock_s3_adapter,
        domain: str,
        dataset: str,
        sensitivity: SensitivityLevel,
        token_scopes: List[str],
        endpoint_scopes: List[str],
    ):
        mock_s3_adapter.get_dataset_sensitivity.return_value = sensitivity
        try:
            match_client_app_permissions(token_scopes, endpoint_scopes, domain, dataset)
        except AuthorisationError:
            pytest.fail("Unexpected Unauthorised Error was thrown")

    @patch("api.application.services.authorisation.authorisation_service.s3_adapter")
    @pytest.mark.parametrize(
        "domain, dataset, sensitivity, token_scopes, endpoint_scopes",
        [
            # Token with no scopes is unauthorised
            ("domain", "dataset", SensitivityLevel.PUBLIC, [], ["READ"]),
            # Token does not have required action privilege
            ("domain", "dataset", SensitivityLevel.PUBLIC, ["READ_PUBLIC"], ["WRITE"]),
            # Token does not have required sensitivity privilege
            ("domain", "dataset", SensitivityLevel.PRIVATE, ["READ_PUBLIC"], ["READ"]),
            (
                "domain",
                "dataset",
                SensitivityLevel.PRIVATE,
                ["WRITE_PUBLIC"],
                ["WRITE"],
            ),
            # Edge combinations
            # WRITE high sensitivity does not imply READ low sensitivity
            ("domain", "dataset", SensitivityLevel.PUBLIC, ["WRITE_PRIVATE"], ["READ"]),
            # WRITE does not imply READ
            ("domain", "dataset", SensitivityLevel.PUBLIC, ["WRITE_ALL"], ["READ"]),
            ("domain", "dataset", SensitivityLevel.PRIVATE, ["WRITE_ALL"], ["READ"]),
        ],
    )
    def test_denies_permissions(
        self,
        mock_s3_adapter,
        domain: str,
        dataset: str,
        sensitivity: SensitivityLevel,
        token_scopes: List[str],
        endpoint_scopes: List[str],
    ):

        mock_s3_adapter.get_dataset_sensitivity.return_value = sensitivity

        with pytest.raises(
            AuthorisationError, match="Not enough permissions to access endpoint"
        ):
            match_client_app_permissions(token_scopes, endpoint_scopes, domain, dataset)


class TestUserPermissionsMatching:
    def setup_method(self):
        self.mock_s3_client = Mock()

    @pytest.mark.parametrize(
        "domain, dataset, token_permissions, endpoint_scopes",
        [
            # Token with correct action and sensitivity privilege
            ("domain", "dataset", ["READ/domain/dataset"], ["READ"]),
            ("domain", "dataset", ["WRITE/domain/dataset"], ["WRITE"]),
        ],
    )
    def test_matches_permissions(
        self,
        domain: str,
        dataset: str,
        token_permissions: List[str],
        endpoint_scopes: List[str],
    ):
        try:
            match_user_permissions(token_permissions, endpoint_scopes, domain, dataset)
        except AuthorisationError:
            pytest.fail("Unexpected Unauthorised Error was thrown")

    @pytest.mark.parametrize(
        "domain, dataset, token_permissions, endpoint_scopes",
        [
            # Token with correct action and sensitivity privilege
            ("domain", "dataset", ["READ/domain/dataset"], ["WRITE"]),
            ("domain", "dataset", ["WRITE/domain/dataset"], ["READ"]),
            ("domain", "dataset", ["READ/domain1/dataset"], ["READ"]),
            ("domain", "dataset", ["WRITE/domain/dataset1"], ["WRITE"]),
        ],
    )
    def test_denies_permissions(
        self,
        domain: str,
        dataset: str,
        token_permissions: List[str],
        endpoint_scopes: List[str],
    ):
        with pytest.raises(
            AuthorisationError, match="Not enough permissions to access endpoint"
        ):
            match_user_permissions(token_permissions, endpoint_scopes, domain, dataset)

    @pytest.mark.parametrize(
        "domain, dataset, token_permissions, endpoint_scopes",
        [
            (None, None, ["READ/domain/dataset"], ["READ"]),
            (None, None, ["WRITE/domain/dataset"], ["WRITE"]),
        ],
    )
    def test_matches_permissions_when_domain_and_dataset_are_none(
        self,
        domain: str,
        dataset: str,
        token_permissions: List[str],
        endpoint_scopes: List[str],
    ):
        try:
            match_user_permissions(token_permissions, endpoint_scopes, domain, dataset)
        except BaseAppException:
            pytest.fail("Unexpected Unauthorised Error was thrown")

    @pytest.mark.parametrize(
        "domain, dataset, token_permissions, endpoint_scopes",
        [
            (None, "dataset", ["READ/domain/dataset"], ["READ"]),
            ("domain", None, ["WRITE/domain/dataset"], ["WRITE"]),
        ],
    )
    def test_raises_error_when_domain_or_dataset_is_none(
        self,
        domain: str,
        dataset: str,
        token_permissions: List[str],
        endpoint_scopes: List[str],
    ):
        with pytest.raises(
            AuthorisationError, match="Not enough permissions to access endpoint"
        ):
            match_user_permissions(token_permissions, endpoint_scopes, domain, dataset)
