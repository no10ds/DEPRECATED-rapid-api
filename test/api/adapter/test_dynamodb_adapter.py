from unittest.mock import Mock, call

import pytest
from boto3.dynamodb.conditions import Key, Attr, Or
from botocore.exceptions import ClientError

from api.adapter.dynamodb_adapter import DynamoDBAdapter
from api.common.config.auth import SubjectType
from api.common.custom_exceptions import (
    AWSServiceError,
    UserError,
    SubjectNotFoundError,
)
from api.domain.permission_item import PermissionItem
from api.domain.subject_permissions import SubjectPermissions


class TestDynamoDBAdapter:
    expected_db_query_response = {
        "Items": [
            {
                "PK": "PERMISSION",
                "SK": "USER_ADMIN",
                "Id": "USER_ADMIN",
                "Type": "USER_ADMIN",
            },
            {
                "PK": "PERMISSION",
                "SK": "READ_ALL",
                "Id": "READ_ALL",
                "Sensitivity": "ALL",
                "Type": "READ",
            },
            {
                "PK": "PERMISSION",
                "SK": "WRITE_ALL",
                "Id": "WRITE_ALL",
                "Sensitivity": "ALL",
                "Type": "WRITE",
            },
            {
                "PK": "PERMISSION",
                "SK": "READ_PRIVATE",
                "Id": "READ_PRIVATE",
                "Sensitivity": "PRIVATE",
                "Type": "READ",
            },
        ],
        "Count": 4,
    }

    def setup_method(self):
        self.dynamo_boto_resource = Mock()
        self.test_permissions_table_name = "TEST PERMISSIONS"
        self.dynamo_adapter = DynamoDBAdapter(self.dynamo_boto_resource)

    def test_store_subject_permissions(self):
        client_id = "123456789"
        client_permissions = ["READ_ALL", "WRITE_ALL", "READ_PRIVATE", "USER_ADMIN"]
        expected_client_permissions = {
            "READ_ALL",
            "WRITE_ALL",
            "READ_PRIVATE",
            "USER_ADMIN",
        }
        self.dynamo_boto_resource.query.return_value = self.expected_db_query_response

        self.dynamo_adapter.store_subject_permissions(
            SubjectType.CLIENT, client_id, client_permissions
        )

        self.dynamo_boto_resource.put_item.assert_called_once_with(
            Item={
                "PK": "SUBJECT",
                "SK": client_id,
                "Id": client_id,
                "Type": "CLIENT",
                "Permissions": expected_client_permissions,
            },
        )

    def test_store_protected_permission(self):
        domain = "TRAIN"
        mock_batch_writer = Mock()
        mock_batch_writer.__enter__ = Mock(return_value=mock_batch_writer)
        mock_batch_writer.__exit__ = Mock(return_value=None)
        self.dynamo_boto_resource.batch_writer.return_value = mock_batch_writer

        permissions = [
            PermissionItem(
                id="READ_PROTECTED_TRAIN",
                type="READ",
                sensitivity="PROTECTED",
                domain=domain,
            ),
            PermissionItem(
                id="WRITE_PROTECTED_TRAIN",
                type="WRITE",
                sensitivity="PROTECTED",
                domain=domain,
            ),
        ]

        self.dynamo_adapter.store_protected_permission(permissions, domain)

        mock_batch_writer.put_item.assert_has_calls(
            (
                call(
                    Item={
                        "PK": "PERMISSION",
                        "SK": "WRITE_PROTECTED_TRAIN",
                        "Id": "WRITE_PROTECTED_TRAIN",
                        "Type": "WRITE",
                        "Sensitivity": "PROTECTED",
                        "Domain": "TRAIN",
                    }
                ),
                call(
                    Item={
                        "PK": "PERMISSION",
                        "SK": "READ_PROTECTED_TRAIN",
                        "Id": "READ_PROTECTED_TRAIN",
                        "Type": "READ",
                        "Sensitivity": "PROTECTED",
                        "Domain": "TRAIN",
                    }
                ),
            ),
            any_order=True,
        )

    def test_store_subject_permissions_throws_error_when_database_call_fails(self):
        subject_id = "123456789"
        permissions = ["READ_ALL", "WRITE_ALL", "READ_PRIVATE", "USER_ADMIN"]
        subject_type = SubjectType.CLIENT
        self.dynamo_boto_resource.query.return_value = self.expected_db_query_response

        self.dynamo_boto_resource.put_item.side_effect = ClientError(
            error_response={"Error": {"Code": "ConditionalCheckFailedException"}},
            operation_name="PutItem",
        )

        with pytest.raises(
            AWSServiceError,
            match=f"Error storing the {subject_type.value}: {subject_id}",
        ):
            self.dynamo_adapter.store_subject_permissions(
                subject_type, subject_id, permissions
            )

    def test_validate_permission_throws_error_when_query_fails(self):
        permissions = ["READ_ALL", "WRITE_ALL", "READ_PRIVATE", "USER_ADMIN"]

        self.dynamo_boto_resource.query.side_effect = ClientError(
            error_response={"Error": {"Code": "ConditionalCheckFailedException"}},
            operation_name="Query",
        )

        with pytest.raises(
            AWSServiceError,
            match="Error fetching permissions from the database",
        ):
            self.dynamo_adapter.validate_permissions(permissions)

    def test_validates_permissions_exist_in_the_database(self):
        test_user_permissions = ["READ_PRIVATE", "WRITE_ALL"]
        self.dynamo_boto_resource.query.return_value = {
            "Items": [
                {
                    "PK": "PERMISSION",
                    "SK": "WRITE_ALL",
                    "Id": "WRITE_ALL",
                    "Sensitivity": "ALL",
                    "Type": "WRITE",
                },
                {
                    "PK": "PERMISSION",
                    "SK": "READ_PRIVATE",
                    "Id": "READ_PRIVATE",
                    "Sensitivity": "PRIVATE",
                    "Type": "READ",
                },
            ],
            "Count": 2,
        }

        try:
            self.dynamo_adapter.validate_permissions(test_user_permissions)
        except UserError:
            pytest.fail("Unexpected UserError was thrown")

        self.dynamo_boto_resource.query.assert_called_once_with(
            KeyConditionExpression=Key("PK").eq("PERMISSION"),
            FilterExpression=Or(
                *[(Attr("Id").eq(value)) for value in test_user_permissions]
            ),
        )

    def test_raises_error_when_attempting_to_validate_at_least_one_invalid_permission(
        self,
    ):
        self.dynamo_boto_resource.query.return_value = {
            "Items": [
                {
                    "PK": "PERMISSION",
                    "SK": "WRITE_ALL",
                    "Id": "WRITE_ALL",
                    "Sensitivity": "ALL",
                    "Type": "WRITE",
                }
            ],
            "Count": 1,
        }

        invalid_permissions = ["READ_SENSITIVE", "ACCESS_ALL", "ADMIN", "FAKE_ADMIN"]
        test_user_permissions = ["WRITE_ALL", *invalid_permissions]

        with pytest.raises(
            UserError,
            match="One or more of the provided permissions is invalid or duplicated",
        ):
            self.dynamo_adapter.validate_permissions(test_user_permissions)

    def test_get_all_permissions(self):
        expected_response = ["USER_ADMIN", "READ_ALL", "WRITE_ALL", "READ_PRIVATE"]
        self.dynamo_boto_resource.query.return_value = self.expected_db_query_response
        actual_response = self.dynamo_adapter.get_all_permissions()

        self.dynamo_boto_resource.query.assert_called_once_with(
            KeyConditionExpression=Key("PK").eq("PERMISSION"),
        )
        assert actual_response == expected_response

    def test_get_all_permissions_raises_error_if_database_call_fails(self):
        self.dynamo_boto_resource.query.side_effect = ClientError(
            error_response={
                "Error": {"Code": "QueryFailedException"},
                "Message": "Failed to execute query: The error message",
            },
            operation_name="Query",
        )

        with pytest.raises(
            AWSServiceError,
            match="Error fetching permissions, please contact your system administrator",
        ):
            self.dynamo_adapter.get_all_permissions()

    def test_get_permissions_for_subject(self):
        subject_id = "test-subject-id"
        self.dynamo_boto_resource.query.return_value = {
            "Items": [
                {
                    "PK": "SUBJECT",
                    "SK": subject_id,
                    "Id": subject_id,
                    "Type": "CLIENT",
                    "Permissions": {
                        "DATA_ADMIN",
                        "READ_ALL",
                        "USER_ADMIN",
                        "WRITE_ALL",
                    },
                }
            ],
            "Count": 1,
        }

        expected_permissions = ["DATA_ADMIN", "READ_ALL", "USER_ADMIN", "WRITE_ALL"]

        response = self.dynamo_adapter.get_permissions_for_subject(subject_id)

        assert sorted(response) == sorted(expected_permissions)

        self.dynamo_boto_resource.query.assert_called_once_with(
            KeyConditionExpression=Key("PK").eq("SUBJECT"),
            FilterExpression=Attr("Id").eq(subject_id),
        )

    def test_get_permissions_for_non_existent_subject(self):
        subject_id = "fake-subject-id"
        self.dynamo_boto_resource.query.return_value = {
            "Items": [],
            "Count": 0,
        }

        with pytest.raises(
            SubjectNotFoundError,
            match="Subject not found in database",
        ):
            self.dynamo_adapter.get_permissions_for_subject(subject_id)

    def test_get_permissions_for_subject_with_no_permissions(self):
        subject_id = "test-subject-id"
        self.dynamo_boto_resource.query.return_value = {
            "Items": [
                {
                    "PK": "SUBJECT",
                    "SK": subject_id,
                    "Id": subject_id,
                    "Type": "CLIENT",
                    "Permissions": "",
                }
            ],
            "Count": 1,
        }

        response = self.dynamo_adapter.get_permissions_for_subject(subject_id)

        assert len(response) == 0

    def test_get_permissions_for_subject_throws_aws_service_error(self):
        subject_id = "test-subject-id"
        self.dynamo_boto_resource.query.side_effect = ClientError(
            error_response={"Error": {"Code": "ConditionalCheckFailedException"}},
            operation_name="Query",
        )

        with pytest.raises(
            AWSServiceError,
            match="Error fetching permissions, please contact your system administrator",
        ):
            self.dynamo_adapter.get_permissions_for_subject(subject_id)

    def test_get_all_protected_permissions(self):
        expected_db_query_response = {
            "Items": [
                {
                    "PK": "PERMISSION",
                    "SK": "WRITE_PROTECTED_DOMAIN",
                    "Id": "WRITE_PROTECTED_DOMAIN",
                    "Sensitivity": "PROTECTED",
                    "Type": "WRITE",
                    "Domain": "DOMAIN",
                },
                {
                    "PK": "PERMISSION",
                    "SK": "READ_PROTECTED_DOMAIN",
                    "Id": "READ_PROTECTED_DOMAIN",
                    "Sensitivity": "PROTECTED",
                    "Type": "READ",
                    "Domain": "DOMAIN",
                },
            ],
            "Count": 2,
        }

        expected_permission_item_list = [
            PermissionItem(
                id="WRITE_PROTECTED_DOMAIN",
                type="WRITE",
                sensitivity="PROTECTED",
                domain="DOMAIN",
            ),
            PermissionItem(
                id="READ_PROTECTED_DOMAIN",
                type="READ",
                sensitivity="PROTECTED",
                domain="DOMAIN",
            ),
        ]

        self.dynamo_boto_resource.query.return_value = expected_db_query_response
        response = self.dynamo_adapter.get_all_protected_permissions()
        assert len(response) == 2
        self.dynamo_boto_resource.query.assert_called_once_with(
            KeyConditionExpression=Key("PK").eq("PERMISSION"),
            FilterExpression=Attr("Sensitivity").eq("PROTECTED"),
        )

        assert response == expected_permission_item_list

    def test_update_subject_permissions(self):
        subject_permissions = SubjectPermissions(
            subject_id="asdf1234678sd", permissions=["READ_ALL"]
        )

        self.dynamo_adapter.update_subject_permissions(subject_permissions)

        self.dynamo_boto_resource.update_item.assert_called_once_with(
            Key={"PK": "SUBJECT", "SK": subject_permissions.subject_id},
            ConditionExpression="SK = :sid",
            UpdateExpression="set #P = :r",
            ExpressionAttributeValues={
                ":r": set(subject_permissions.permissions),
                ":sid": subject_permissions.subject_id,
            },
            ExpressionAttributeNames={"#P": "Permissions"},
        )

    def test_update_subject_permissions_on_service_error(self):
        subject_permissions = SubjectPermissions(
            subject_id="asdf1234678sd", permissions=["READ_ALL"]
        )
        self.dynamo_boto_resource.update_item.side_effect = ClientError(
            error_response={"Error": {"Code": "SomeOtherError"}},
            operation_name="UpdateItem",
        )

        with pytest.raises(
            AWSServiceError,
            match=f"Error updating permissions for {subject_permissions.subject_id}",
        ):
            self.dynamo_adapter.update_subject_permissions(subject_permissions)

    def test_user_error_when_user_does_not_exist_in_database(self):
        subject_permissions = SubjectPermissions(
            subject_id="asdf1234678sd", permissions=["READ_ALL"]
        )
        self.dynamo_boto_resource.update_item.side_effect = ClientError(
            error_response={"Error": {"Code": "ConditionalCheckFailedException"}},
            operation_name="UpdateItem",
        )

        with pytest.raises(
            UserError, match="Subject with ID asdf1234678sd does not exist"
        ):
            self.dynamo_adapter.update_subject_permissions(subject_permissions)
