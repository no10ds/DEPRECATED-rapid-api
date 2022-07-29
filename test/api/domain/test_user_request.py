import pytest

from api.common.custom_exceptions import UserError
from api.domain.user import UserRequest


class TestUserRequest:
    @pytest.mark.parametrize(
        "provided_username",
        [
            "username_name",
            "username_name2",
            "username@email.com",
            "VA.li_d@na-mE",
            "A....",
            "S1234",
        ],
    )
    def test_get_validated_username(self, provided_username):
        request = UserRequest(username=provided_username, email="user@email.com")

        try:
            validated_name = request.get_validated_username()
            assert validated_name == provided_username
        except UserError:
            pytest.fail("An unexpected UserError was thrown")

    @pytest.mark.parametrize(
        "provided_username",
        [
            "",
            " ",
            "SOme naME",
            "sOMe!name",
            "-some-nAMe",
            "(some)namE",
            "1234",
            "....",
            "A" * 2,
            "A" * 129,
        ],
    )
    def test_raises_error_when_invalid_username(self, provided_username):
        request = UserRequest(username=provided_username, email="user@email.com")

        with pytest.raises(UserError, match="Invalid user name provided"):
            request.get_validated_username()
