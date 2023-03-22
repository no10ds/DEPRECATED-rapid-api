from typing import List

import pytest

from api.application.services.authorisation.acceptable_permissions import (
    AcceptablePermissions,
    generate_acceptable_scopes,
)
from api.common.config.auth import SensitivityLevel


class TestAcceptablePermissions:
    @pytest.mark.parametrize(
        "accepted_scopes, token_scopes",
        [
            # READ endpoint
            (
                AcceptablePermissions(
                    required=set(), optional={"READ_ALL", "READ_ALL_PUBLIC"}
                ),
                ["READ_ALL_PUBLIC"],
            ),
            # WRITE endpoint
            (
                AcceptablePermissions(required=set(), optional={"WRITE_LAYER_PUBLIC"}),
                ["WRITE_LAYER_PUBLIC"],
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


class TestAcceptablePermissionsGeneration:
    @pytest.mark.parametrize(
        "domain, layer, sensitivity, endpoint_scopes, acceptable_scopes",
        [
            (
                "domain",
                "raw",
                SensitivityLevel.PUBLIC,
                ["READ"],
                AcceptablePermissions(  # noqa: E126
                    required=set(),
                    optional={
                        "READ_ALL",
                        "READ_RAW_PUBLIC",
                        "READ_RAW_PRIVATE",
                        "READ_ALL_PUBLIC",
                        "READ_ALL_PRIVATE",
                    },
                ),
            ),
            (
                "domain",
                "layer",
                SensitivityLevel.PUBLIC,
                ["USER_ADMIN", "READ"],
                AcceptablePermissions(  # noqa: E126
                    required={"USER_ADMIN"},
                    optional={
                        "READ_ALL",
                        "READ_LAYER_PUBLIC",
                        "READ_LAYER_PRIVATE",
                        "READ_ALL_PUBLIC",
                        "READ_ALL_PRIVATE",
                    },
                ),
            ),
            (
                "domain",
                "raw",
                SensitivityLevel.PRIVATE,
                ["USER_ADMIN", "READ", "WRITE"],  # noqa: E126
                AcceptablePermissions(  # noqa: E126
                    required={"USER_ADMIN"},
                    optional={
                        "READ_ALL",
                        "WRITE_ALL",
                        "READ_ALL_PRIVATE",
                        "WRITE_ALL_PRIVATE",
                        "READ_RAW_PRIVATE",
                        "WRITE_RAW_PRIVATE",
                    },
                ),
            ),
            (
                None,
                None,
                None,
                ["USER_ADMIN"],
                AcceptablePermissions(
                    required={"USER_ADMIN"}, optional=set()
                ),  # noqa: E126
            ),
            (
                "domain",
                "layer",
                SensitivityLevel.PROTECTED,
                ["WRITE"],
                AcceptablePermissions(
                    required=set(),
                    optional={
                        "WRITE_ALL",
                        "WRITE_ALL_PROTECTED_DOMAIN",
                        "WRITE_LAYER_PROTECTED_DOMAIN",
                    },
                ),  # noqa: E126
            ),
        ],
    )
    def test_generate_acceptable_permissions(
        self,
        domain: str,
        layer: str,
        sensitivity: SensitivityLevel,
        endpoint_scopes: List[str],
        acceptable_scopes: AcceptablePermissions,
    ):
        result = generate_acceptable_scopes(endpoint_scopes, sensitivity, layer, domain)
        assert result == acceptable_scopes
