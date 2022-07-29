from unittest.mock import Mock

import pytest

from api.application.services.subject_service import SubjectService
from api.common.config.auth import SubjectType
from api.common.custom_exceptions import AWSServiceError, UserError
from api.domain.client import ClientRequest, ClientResponse
from api.domain.user import UserResponse, UserRequest


class TestClientCreation:
    def setup_method(self):
        self.cognito_adapter = Mock()
        self.dynamo_adapter = Mock()
        self.subject_service = SubjectService(self.cognito_adapter, self.dynamo_adapter)

    def test_create_client(self):
        expected_response = ClientResponse(
            client_name="my_client",
            client_id="some-client-id",
            client_secret="some-client-secret",  # pragma: allowlist secret
            permissions=["WRITE_PUBLIC", "READ_PRIVATE"],
        )

        client_request = ClientRequest(
            client_name="my_client", permissions=["WRITE_PUBLIC", "READ_PRIVATE"]
        )

        self.cognito_adapter.create_client_app.return_value = expected_response

        client_response = self.subject_service.create_client(client_request)

        self.cognito_adapter.create_client_app.assert_called_once_with(client_request)

        self.dynamo_adapter.validate_permissions.assert_called_once_with(
            client_request.permissions
        )

        self.dynamo_adapter.store_subject_permissions.assert_called_once_with(
            SubjectType.CLIENT, expected_response.client_id, client_request.permissions
        )

        assert client_response == expected_response

    def test_do_not_create_client_when_validate_permissions_fails(self):
        client_request = ClientRequest(
            client_name="my_client", permissions=["WRITE_PUBLIC", "READ_PRIVATE"]
        )
        self.dynamo_adapter.validate_permissions.side_effect = AWSServiceError(
            "The client could not be created, please contact your system administrator"
        )

        with pytest.raises(
            AWSServiceError,
            match="The client could not be created, please contact your system administrator",
        ):
            self.subject_service.create_client(client_request)

        self.dynamo_adapter.store_subject_permissions.assert_not_called()
        self.cognito_adapter.create_client_app.assert_not_called()

    def test_do_not_create_client_when_invalid_permissions(self):
        client_request = ClientRequest(
            client_name="my_client", permissions=["WRITE_PUBLIC", "READ_PRIVATE"]
        )
        self.dynamo_adapter.validate_permissions.side_effect = UserError(
            "One or more of the provided permissions do not exist"
        )

        with pytest.raises(
            UserError,
            match="One or more of the provided permissions do not exist",
        ):
            self.subject_service.create_client(client_request)

        self.dynamo_adapter.store_subject_permissions.assert_not_called()
        self.cognito_adapter.create_client_app.assert_not_called()

    def test_delete_existing_client_when_db_fails(self):

        expected_response = ClientResponse(
            client_name="my_client",
            client_id="some-client-id",
            client_secret="some-client-secret",  # pragma: allowlist secret
            permissions=["WRITE_PUBLIC", "READ_PRIVATE"],
        )

        client_request = ClientRequest(
            client_name="my_client", permissions=["WRITE_PUBLIC", "READ_PRIVATE"]
        )
        self.cognito_adapter.create_client_app.return_value = expected_response
        self.dynamo_adapter.store_subject_permissions.side_effect = AWSServiceError(
            "The client could not be created, please contact your system administrator"
        )

        with pytest.raises(
            AWSServiceError,
            match="The client could not be created, please contact your system administrator",
        ):
            self.subject_service.create_client(client_request)

        self.cognito_adapter.create_client_app.assert_called_once_with(client_request)
        self.cognito_adapter.delete_client_app.assert_called_once_with("some-client-id")


class TestUserCreation:
    def setup_method(self):
        self.cognito_adapter = Mock()
        self.dynamo_adapter = Mock()
        self.subject_service = SubjectService(self.cognito_adapter, self.dynamo_adapter)

    def test_create_subject(self):
        expected_response = UserResponse(
            username="user-name",
            email="user-name@some-email.com",
            permissions=["WRITE_PUBLIC", "READ_PRIVATE"],
            user_id="some-uu-id-b226-e5fd18c59b85",
        )
        subject_request = UserRequest(
            username="user-name",
            email="user-name@some-email.com",
            permissions=["WRITE_PUBLIC", "READ_PRIVATE"],
        )

        self.cognito_adapter.create_user.return_value = expected_response

        actual_response = self.subject_service.create_user(subject_request)

        self.dynamo_adapter.validate_permissions.assert_called_once_with(
            subject_request.permissions
        )

        self.cognito_adapter.create_user.assert_called_once_with(subject_request)

        self.dynamo_adapter.store_subject_permissions.assert_called_once_with(
            SubjectType.USER, expected_response.user_id, subject_request.permissions
        )

        assert actual_response == expected_response
