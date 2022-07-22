import pytest

from api.domain.token import Token


@pytest.fixture
def valid_client_token_payload():
    yield {
        "sub": "the-client-subject",
        "scope": ["scope1", "scope2"]
    }


@pytest.fixture
def valid_user_token_payload():
    yield {
        "sub": "the-user-subject",
        "cognito:groups": ["group1", "group2"]
    }


class TestSubjectExtraction:

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

    def test_extracts_client_app_scopes_when_available_and_valid(self, valid_client_token_payload):
        token = Token(valid_client_token_payload)

        assert token.permissions == ["scope1", "scope2"]
        assert token.is_client_token() is True
        assert token.is_user_token() is False

    def test_extracts_user_groups_when_available_and_valid(self, valid_user_token_payload):
        token = Token(valid_user_token_payload)

        assert token.permissions == ["group1", "group2"]
        assert token.is_client_token() is False
        assert token.is_user_token() is True

    def test_favours_client_scopes_over_user_groups_if_both_available(self):
        payload = {
            "sub": "the-client-subject",
            "scope": ["scope1", "scope2"],
            "cognito:groups": ["group1", "group2"]
        }
        token = Token(payload)

        assert token.permissions == ["group1", "group2"]
        assert token.is_client_token() is False
        assert token.is_user_token() is True

    def test_raises_error_when_neither_scopes_or_groups_exist(self):
        payload = {
            "sub": "the-subject"
        }

        with pytest.raises(ValueError, match="No permissions found"):
            Token(payload)

    def test_raises_error_when_subject_field_empty(self):
        payload = {
            "sub": "the-client-subject",
            "scope": None,
            "cognito:groups": None
        }

        with pytest.raises(ValueError):
            Token(payload)
