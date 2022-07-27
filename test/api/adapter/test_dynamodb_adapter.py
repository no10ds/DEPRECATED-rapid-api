from unittest.mock import Mock

import pytest
from boto3.dynamodb.conditions import Key, Attr, Or
from botocore.exceptions import ClientError

from api.adapter.dynamodb_adapter import DynamoDBAdapter
from api.common.custom_exceptions import AWSServiceError, UserError


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

    def test_create_client_item(self):
        client_id = "123456789"
        client_permissions = ["READ_ALL", "WRITE_ALL", "READ_PRIVATE", "USER_ADMIN"]
        expected_client_permissions = {
            "READ_ALL",
            "WRITE_ALL",
            "READ_PRIVATE",
            "USER_ADMIN",
        }
        self.dynamo_boto_resource.query.return_value = self.expected_db_query_response
        self.dynamo_adapter.create_client_item(client_id, client_permissions)

        self.dynamo_boto_resource.put_item.assert_called_once_with(
            Item={
                "PK": "SUBJECT",
                "SK": client_id,
                "Id": client_id,
                "Type": "CLIENT",
                "Permissions": expected_client_permissions,
            },
        )

    def test_create_client_throws_error_when_db_fails(self):
        client_id = "123456789"
        client_permissions = ["READ_ALL", "WRITE_ALL", "READ_PRIVATE", "USER_ADMIN"]
        self.dynamo_boto_resource.query.return_value = self.expected_db_query_response

        self.dynamo_boto_resource.put_item.side_effect = ClientError(
            error_response={"Error": {"Code": "ConditionalCheckFailedException"}},
            operation_name="PutItem",
        )

        with pytest.raises(
            AWSServiceError,
            match="The client could not be created, please contact your system administrator",
        ):
            self.dynamo_adapter.create_client_item(client_id, client_permissions)

    def test_create_client_throws_error_when_query_fails(self):
        client_id = "123456789"
        client_permissions = ["READ_ALL", "WRITE_ALL", "READ_PRIVATE", "USER_ADMIN"]
        self.dynamo_boto_resource.query.return_value = self.expected_db_query_response

        self.dynamo_boto_resource.query.side_effect = ClientError(
            error_response={"Error": {"Code": "ConditionalCheckFailedException"}},
            operation_name="PutItem",
        )

        with pytest.raises(
            AWSServiceError,
            match="The client could not be created, please contact your system administrator",
        ):
            self.dynamo_adapter.create_client_item(client_id, client_permissions)

    def test_create_client_throws_error_when_invalid_permissions(self):
        client_id = "123456789"
        client_permissions = ["READ_ALL", "WRITE_ALL"]
        self.dynamo_boto_resource.query.return_value = self.expected_db_query_response

        with pytest.raises(
            UserError, match="One or more of the provided permissions do not exist"
        ):
            self.dynamo_adapter.create_client_item(client_id, client_permissions)

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

        self.dynamo_adapter.create_subject_permission(
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
            self.dynamo_adapter.create_subject_permission(
                subject_type, subject_id, permissions
            )

    def test_create_subject_throws_error_when_query_fails(self):
        subject_id = "123456789"
        permissions = ["READ_ALL", "WRITE_ALL", "READ_PRIVATE", "USER_ADMIN"]
        subject_type = "CLIENT"
        self.dynamo_boto_resource.query.return_value = self.expected_db_query_response

        self.dynamo_boto_resource.query.side_effect = ClientError(
            error_response={"Error": {"Code": "ConditionalCheckFailedException"}},
            operation_name="Query",
        )

        with pytest.raises(
            AWSServiceError,
            match="The client could not be created, please contact your system administrator",
        ):
            self.dynamo_adapter.create_subject_permission(
                subject_type, subject_id, permissions
            )

    def test_create_subject_throws_error_when_invalid_permissions(self):
        subject_type = "CLIENT"
        subject_id = "123456789"
        permissions = ["READ_ALL", "WRITE_ALL"]
        self.dynamo_boto_resource.query.return_value = self.expected_db_query_response

        with pytest.raises(
            UserError, match="One or more of the provided permissions do not exist"
        ):
            self.dynamo_adapter.create_subject_permission(
                subject_type, subject_id, permissions
            )

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

        self.dynamo_adapter.validate_permission(test_user_permissions)
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
            UserError, match="One or more of the provided permissions do not exist"
        ):
            self.dynamo_adapter.validate_permission(test_user_permissions)
