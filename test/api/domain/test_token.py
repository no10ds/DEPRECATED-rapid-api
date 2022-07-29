from unittest.mock import patch

import pytest

from api.domain.token import Token


@pytest.fixture
def valid_client_token_payload():
    yield {
        "sub": "the-client-subject",
        "scope": "https://example.com/scope1 https://example.com/scope2",
    }


@pytest.fixture
def valid_user_token_payload():
    yield {"sub": "the-user-subject", "cognito:groups": ["group1", "group2"]}


class TestSubjectExtraction:
    @patch("api.domain.token.COGNITO_RESOURCE_SERVER_ID", "https://example.com")
    def test_extract_subject_when_available_and_valid(self, valid_client_token_payload):
        token = Token(valid_client_token_payload)

        assert token.subject == "the-client-subject"

    def test_raises_error_when_no_subject_field(self):
        payload = {}

        with pytest.raises(ValueError):
            Token(payload)

    def test_raises_error_when_subject_field_empty(self):
        payload = {"sub": None}

        with pytest.raises(ValueError):
            Token(payload)


class TestPermissionsExtraction:
    @patch("api.domain.token.COGNITO_RESOURCE_SERVER_ID", "https://example.com")
    def test_extracts_client_app_scopes_when_available_and_valid(
        self, valid_client_token_payload
    ):
        token = Token(valid_client_token_payload)

        assert token.permissions == ["scope1", "scope2"]

    def test_extracts_user_groups_when_available_and_valid(
        self, valid_user_token_payload
    ):
        token = Token(valid_user_token_payload)

        assert token.permissions == []

    def test_returns_empty_permissions_when_neither_scopes_or_groups_exist(self):
        payload = {"sub": "the-subject"}

        token = Token(payload)

        assert token.permissions == []

    def test_returns_empty_permissions_when_scope_and_groups_fields_empty(self):
        payload = {"sub": "the-client-subject", "scope": None, "cognito:groups": None}

        token = Token(payload)

        assert token.permissions == []

    @patch("api.domain.token.COGNITO_RESOURCE_SERVER_ID", "https://example.com")
    def test_raises_error_when_scopes_incorrectly_formatted(self):
        payload = {
            "sub": "the-client-subject",
            "scope": "https://not-example.com/scope1 https://not-example.com/scope2",
            "cognito:groups": None,
        }

        with pytest.raises(ValueError, match="Invalid scope field"):
            Token(payload)
