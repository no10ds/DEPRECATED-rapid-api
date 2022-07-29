from unittest.mock import Mock

import pytest
from boto3.dynamodb.conditions import Key, Attr, Or
from botocore.exceptions import ClientError

from api.adapter.dynamodb_adapter import DynamoDBAdapter
from api.common.custom_exceptions import (
    AWSServiceError,
    UserError,
    SubjectNotFoundError,
)


class TestDynamoDBAdapter:
    expected_db_query_response = {
        "Items": [
            {
                "PK": {"S": "PERMISSION"},
                "SK": {"S": "USER_ADMIN"},
                "Type": {"S": "USER_ADMIN"},
            },
            {
                "PK": {"S": "PERMISSION"},
                "SK": {"S": "READ_ALL"},
                "Sensitivity": {"S": "ALL"},
                "Type": {"S": "READ"},
            },
            {
                "PK": {"S": "PERMISSION"},
                "SK": {"S": "WRITE_ALL"},
                "Sensitivity": {"S": "ALL"},
                "Type": {"S": "WRITE"},
            },
            {
                "PK": {"S": "PERMISSION"},
                "SK": {"S": "READ_PRIVATE"},
                "Sensitivity": {"S": "PRIVATE"},
                "Type": {"S": "READ"},
            },
        ],
        "Count": 4,
    }

    def setup_method(self):
        self.dynamo_boto_resource = Mock()
        self.test_permissions_table_name = "TEST PERMISSIONS"
        self.dynamo_adapter = DynamoDBAdapter(self.dynamo_boto_resource)

    def test_create_subject(self):
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
            "CLIENT", client_id, client_permissions
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

    def test_create_subject_throws_error(self):
        subject_id = "123456789"
        permissions = ["READ_ALL", "WRITE_ALL", "READ_PRIVATE", "USER_ADMIN"]
        subject_type = "CLIENT"
        self.dynamo_boto_resource.query.return_value = self.expected_db_query_response

        self.dynamo_boto_resource.put_item.side_effect = ClientError(
            error_response={"Error": {"Code": "ConditionalCheckFailedException"}},
            operation_name="PutItem",
        )

        with pytest.raises(
            AWSServiceError,
            match="The client could not be created, please contact your system administrator",
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
            match="The client could not be created, please contact your system administrator",
        ):
            self.dynamo_adapter.validate_permissions(permissions)

    def test_validates_permissions(self):
        test_user_permissions = ["READ_PRIVATE", "WRITE_ALL"]
        self.dynamo_boto_resource.query.return_value = {
            "Items": [
                {
                    "PK": {"S": "PERMISSION"},
                    "SK": {"S": "WRITE_ALL"},
                    "Id": {"S": "WRITE_ALL"},
                    "Sensitivity": {"S": "ALL"},
                    "Type": {"S": "WRITE"},
                },
                {
                    "PK": {"S": "PERMISSION"},
                    "SK": {"S": "READ_PRIVATE"},
                    "Id": {"S": "READ_PRIVATE"},
                    "Sensitivity": {"S": "PRIVATE"},
                    "Type": {"S": "READ"},
                },
            ],
            "Count": 2,
        }

        self.dynamo_adapter.validate_permissions(test_user_permissions)
        self.dynamo_boto_resource.query.assert_called_once_with(
            KeyConditionExpression=Key("PK").eq("PERMISSION"),
            FilterExpression=Or(
                *[(Attr("Id").eq(value)) for value in test_user_permissions]
            ),
        )

    def test_invalid_permissions(
        self,
    ):
        self.dynamo_boto_resource.query.return_value = {
            "Items": [
                {
                    "PK": {"S": "PERMISSION"},
                    "SK": {"S": "WRITE_ALL"},
                    "Sensitivity": {"S": "ALL"},
                    "Type": {"S": "WRITE"},
                }
            ],
            "Count": 1,
        }

        test_user_permissions = [
            "READ_SENSITIVE",
            "WRITE_ALL",
            "ACCESS_ALL",
            "ADMIN",
            "FAKE_ADMIN",
        ]
        with pytest.raises(
            UserError,
            match="One or more of the provided permissions is invalid or duplicated",
        ):
            self.dynamo_adapter.validate_permissions(test_user_permissions)

    def test_get_permissions_for_subject(self):
        subject_id = "test-subject-id"
        self.dynamo_boto_resource.query.return_value = {
            "Items": [
                {
                    "PK": {"S": "SUBJECT"},
                    "SK": {"S": subject_id},
                    "Id": {"S": subject_id},
                    "Type": {"S": "CLIENT"},
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

        response = self.dynamo_adapter.get_permissions_for_subject(subject_id)

        assert set(response).issubset(
            {"DATA_ADMIN", "READ_ALL", "USER_ADMIN", "WRITE_ALL"}
        )
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
                    "PK": {"S": "SUBJECT"},
                    "SK": {"S": subject_id},
                    "Id": {"S": subject_id},
                    "Type": {"S": "CLIENT"},
                    "Permissions": {},
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
