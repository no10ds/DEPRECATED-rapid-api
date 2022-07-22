from typing import List
from unittest.mock import patch, Mock, ANY

import pytest
from fastapi import HTTPException
from fastapi.security import SecurityScopes
from jwt.exceptions import InvalidTokenError

from api.application.services.authorisation_service import (
    generate_acceptable_scopes,
    match_client_app_permissions,
    AcceptedScopes,
    match_user_permissions,
    extract_client_app_scopes,
    extract_user_groups,
    protect_dataset_endpoint,
    secure_dataset_endpoint,
    parse_token,
    check_credentials_availability,
)
from api.common.config.auth import SensitivityLevel
from api.common.config.aws import DOMAIN_NAME
from api.common.custom_exceptions import (
    AuthorisationError,
    SchemaNotFoundError,
    UserCredentialsUnavailableError,
    BaseAppException,
)
from api.domain.token import Token


class TestExtractingPermissions:
    @patch("jwt.decode")
    @patch("api.application.services.authorisation_service.jwks_client")
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

    @patch("jwt.decode")
    @patch("api.application.services.authorisation_service.jwks_client")
    def test_extract_token_permissions_for_users(self, mock_jwks_client, mock_decode):
        mock_signing_key = Mock()
        mock_signing_key.key = "secret"
        mock_jwks_client.get_signing_key_from_jwt.return_value = mock_signing_key

        mock_decode.return_value = {
            "cognito:groups": ["READ/domain/dataset", "WRITE/domain/dataset"],
            "scope": "phone openid email",
        }

        token_scopes = extract_user_groups(None)

        mock_jwks_client.get_signing_key_from_jwt.assert_called_once_with(None)
        mock_decode.assert_called_once_with(None, "secret", algorithms=["RS256"])
        assert token_scopes == ["READ/domain/dataset", "WRITE/domain/dataset"]

    def test_extract_scopes_from_invalid_client_app_token(self):
        token = "invalid-token"
        with pytest.raises(
            AuthorisationError,
            match="Not enough permissions or access token is missing/invalid",
        ):
            extract_client_app_scopes(token)

    def test_extract_scopes_from_invalid_user_token(self):
        token = "invalid-token"
        with pytest.raises(
            AuthorisationError,
            match="Not enough permissions or access token is missing/invalid",
        ):
            extract_user_groups(token)

    @patch("jwt.decode")
    @patch("api.application.services.authorisation_service.jwks_client")
    def test_extract_handles_valid_client_app_token_with_invalid_payload(
        self, mock_jwks_client, mock_decode
    ):
        mock_signing_key = Mock()
        mock_signing_key.key = "secret"
        mock_jwks_client.get_signing_key_from_jwt.return_value = mock_signing_key

        mock_decode.return_value = {
            "invalid": ["read/domain/dataset", "write/domain/dataset"]
        }

        with pytest.raises(
            AuthorisationError,
            match="Not enough permissions or access token is missing/invalid",
        ):
            extract_client_app_scopes(None)

    @patch("jwt.decode")
    @patch("api.application.services.authorisation_service.jwks_client")
    def test_extract_handles_valid_user_token_with_invalid_payload(
        self, mock_jwks_client, mock_decode
    ):
        mock_signing_key = Mock()
        mock_signing_key.key = "secret"
        mock_jwks_client.get_signing_key_from_jwt.return_value = mock_signing_key

        mock_decode.return_value = {
            "invalid": ["read/domain/dataset", "write/domain/dataset"]
        }

        with pytest.raises(
            AuthorisationError,
            match="Not enough permissions or access token is missing/invalid",
        ):
            extract_user_groups(None)


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

    @patch("api.application.services.authorisation_service.parse_token")
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

    @patch("api.application.services.authorisation_service.check_permissions")
    @patch("api.application.services.authorisation_service.parse_token")
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
    @patch("api.application.services.authorisation_service.check_permissions")
    @patch("api.application.services.authorisation_service.parse_token")
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


class TestParseToken:
    @patch("api.application.services.authorisation_service.Token")
    @patch(
        "api.application.services.authorisation_service._get_validated_token_payload"
    )
    def test_parses_user_token_with_groups(self, mock_token_payload, mock_token):
        token = "user-token"

        payload = {
            "sub": "the-user-id",
            "cognito:groups": ["group1", "group2"],
            "scope": "scope1 scope2 scope3",
        }

        mock_token_payload.return_value = payload

        parse_token(token)

        mock_token_payload.assert_called_once_with("user-token")
        mock_token.assert_called_once_with(payload)

    @patch("api.domain.token.COGNITO_RESOURCE_SERVER_ID", "https://example.com")
    @patch("api.application.services.authorisation_service.Token")
    @patch(
        "api.application.services.authorisation_service._get_validated_token_payload"
    )
    def test_parses_client_token_with_scopes(self, mock_token_payload, mock_token):
        token = "client-token"

        payload = {
            "sub": "the-client-id",
            "scope": "https://example.com/scope1 https://example.com/scope2",
        }

        mock_token_payload.return_value = payload

        parse_token(token)

        mock_token_payload.assert_called_once_with("client-token")
        mock_token.assert_called_once_with(payload)

    @patch("api.application.services.authorisation_service.Token")
    @patch(
        "api.application.services.authorisation_service._get_validated_token_payload"
    )
    def test_parses_user_token_with_no_permissions(
        self, mock_token_payload, mock_token
    ):
        token = "user-token"

        payload = {
            "sub": "the-user-id",
            "scope": "scope1 scope2 scope3",
        }

        mock_token_payload.return_value = payload

        parse_token(token)

        mock_token_payload.assert_called_once_with("user-token")
        mock_token.assert_called_once_with(payload)

    @patch("api.application.services.authorisation_service.Token")
    @patch(
        "api.application.services.authorisation_service._get_validated_token_payload"
    )
    def test_passes_errors_through(self, _mock_token_payload, mock_token):
        mock_token.side_effect = ValueError("Error detail")

        with pytest.raises(ValueError, match="Error detail"):
            parse_token("user-token")


class TestProtectEndpoint:
    @patch("api.application.services.authorisation_service.jwks_client")
    @patch("jwt.decode")
    @patch("api.application.services.authorisation_service.match_user_permissions")
    def test_matches_user_permissions_when_user_token_provided_from_any_source(
        self, mock_match_user_permissions, mock_decode, _mock_jwks_client
    ):
        browser_request = False
        mock_decode.return_value = {
            "cognito:groups": ["READ/domain/dataset", "WRITE/domain/dataset"],
            "scope": "phone openid email",
        }
        protect_dataset_endpoint(
            security_scopes=SecurityScopes(scopes=["READ"]),
            browser_request=browser_request,
            client_token=None,
            user_token="token",
            domain="mydomain",
            dataset="mydataset",
        )

        mock_decode.assert_called_once_with("token", ANY, algorithms=["RS256"])
        mock_match_user_permissions.assert_called_once_with(
            ["READ/domain/dataset", "WRITE/domain/dataset"],
            ["READ"],
            "mydomain",
            "mydataset",
        )

    @patch("api.application.services.authorisation_service.match_user_permissions")
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

    @patch("api.application.services.authorisation_service.jwks_client")
    @patch("jwt.decode")
    @patch(
        "api.application.services.authorisation_service.match_client_app_permissions"
    )
    def test_matches_client_permissions_when_client_token_provided_from_programmatic_client(
        self, match_client_app_permissions, mock_decode, _mock_jwks_client
    ):
        browser_request = False
        mock_decode.return_value = {
            "scope": f"https://{DOMAIN_NAME}/READ_PUBLIC https://{DOMAIN_NAME}/WRITE_PUBLIC"
        }
        protect_dataset_endpoint(
            security_scopes=SecurityScopes(scopes=["READ"]),
            browser_request=browser_request,
            client_token="token",
            user_token=None,
            domain="mydomain",
            dataset="mydataset",
        )

        mock_decode.assert_called_once_with("token", ANY, algorithms=["RS256"])
        match_client_app_permissions.assert_called_once_with(
            ["READ_PUBLIC", "WRITE_PUBLIC"], ["READ"], "mydomain", "mydataset"
        )

    @patch("api.application.services.authorisation_service.jwks_client")
    @patch("jwt.decode")
    @patch(
        "api.application.services.authorisation_service.match_client_app_permissions"
    )
    def test_raises_exception_when_schema_not_found_for_dataset(
        self, match_client_app_permissions, mock_decode, _mock_jwks_client
    ):
        browser_request = False
        mock_decode.return_value = {
            "scope": f"https://{DOMAIN_NAME}/READ_PUBLIC https://{DOMAIN_NAME}/WRITE_PUBLIC"
        }
        match_client_app_permissions.side_effect = SchemaNotFoundError()

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


class TestAcceptedScopes:
    @pytest.mark.parametrize(
        "accepted_scopes, token_scopes",
        [
            # READ endpoint
            (
                AcceptedScopes(required=set(), optional={"READ_ALL", "READ_PUBLIC"}),
                ["READ_PUBLIC"],
            ),
            # WRITE endpoint
            (
                AcceptedScopes(required=set(), optional={"WRITE_PUBLIC"}),
                ["WRITE_PUBLIC"],
            ),
            # Standalone action endpoints
            (AcceptedScopes(required={"USER_ADMIN"}, optional=set()), ["USER_ADMIN"]),
            (AcceptedScopes(required={"DATA_ADMIN"}, optional=set()), ["DATA_ADMIN"]),
        ],
    )
    def test_scopes_satisfy_acceptable_scopes(
        self, accepted_scopes: AcceptedScopes, token_scopes: List[str]
    ):
        assert accepted_scopes.satisfied_by(token_scopes) is True

    @pytest.mark.parametrize(
        "accepted_scopes, token_scopes",
        [
            # READ endpoint
            (
                AcceptedScopes(required=set(), optional={"READ_ALL", "READ_PUBLIC"}),
                [],
            ),  # No token scopes
            # WRITE endpoint
            (
                AcceptedScopes(required=set(), optional={"WRITE_PUBLIC"}),
                ["READ_PUBLIC", "READ_ALL"],
            ),
            # Standalone action endpoints
            (AcceptedScopes(required={"USER_ADMIN"}, optional=set()), ["READ_ALL"]),
            (AcceptedScopes(required={"DATA_ADMIN"}, optional=set()), ["WRITE_ALL"]),
        ],
    )
    def test_scopes_do_not_satisfy_acceptable_scopes(
        self, accepted_scopes: AcceptedScopes, token_scopes: List[str]
    ):
        assert accepted_scopes.satisfied_by(token_scopes) is False


class TestAppPermissionsMatching:
    def setup_method(self):
        self.mock_s3_client = Mock()

    @patch("api.application.services.authorisation_service.s3_adapter")
    @pytest.mark.parametrize(
        "domain, dataset, sensitivity, endpoint_scopes, acceptable_scopes",
        [
            (
                "domain",
                "dataset",
                SensitivityLevel.PUBLIC,
                ["READ"],
                AcceptedScopes(  # noqa: E126
                    required=set(),
                    optional={
                        "READ_ALL",
                        "READ_PUBLIC",
                        "READ_PRIVATE",
                    },
                ),
            ),
            (
                "domain",
                "dataset",
                SensitivityLevel.PUBLIC,
                ["USER_ADMIN", "READ"],
                AcceptedScopes(  # noqa: E126
                    required={"USER_ADMIN"},
                    optional={
                        "READ_ALL",
                        "READ_PUBLIC",
                        "READ_PRIVATE",
                    },
                ),
            ),
            (
                "domain",
                "dataset",
                SensitivityLevel.PRIVATE,
                ["USER_ADMIN", "READ", "WRITE"],  # noqa: E126
                AcceptedScopes(  # noqa: E126
                    required={"USER_ADMIN"},
                    optional={
                        "READ_ALL",
                        "WRITE_ALL",
                        "READ_PRIVATE",
                        "WRITE_PRIVATE",
                    },
                ),
            ),
            (
                None,
                None,
                None,
                ["USER_ADMIN"],
                AcceptedScopes(required={"USER_ADMIN"}, optional=set()),  # noqa: E126
            ),
        ],
    )
    def test_generate_acceptable_scopes(
        self,
        mock_s3_adapter,
        domain: str,
        dataset: str,
        sensitivity: SensitivityLevel,
        endpoint_scopes: List[str],
        acceptable_scopes: AcceptedScopes,
    ):
        mock_s3_adapter.get_dataset_sensitivity.return_value = sensitivity
        result = generate_acceptable_scopes(endpoint_scopes, domain, dataset)
        assert result == acceptable_scopes

    @patch("api.application.services.authorisation_service.s3_adapter")
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

    @patch("api.application.services.authorisation_service.s3_adapter")
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
