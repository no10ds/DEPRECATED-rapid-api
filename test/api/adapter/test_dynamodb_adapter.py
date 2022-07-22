from unittest.mock import Mock

import pytest
from botocore.exceptions import ClientError

from api.adapter.dynamodb_adapter import DynamoDBAdapter
from api.common.custom_exceptions import UserError, AWSServiceError
from api.domain.permission_item import PermissionItem


class TestDynamoDBAdapter:
    expected_db_scan_response = {
        "Items": [
            {
                "SK": {"S": "PER#0"},
                "Id": {"S": "0"},
                "PK": {"S": "PER#USER_ADMIN"},
                "Type": {"S": "USER_ADMIN"},
            },
            {
                "Sensitivity": {"S": "ALL"},
                "SK": {"S": "PER#1"},
                "Id": {"S": "1"},
                "PK": {"S": "PER#READ"},
                "Type": {"S": "READ"},
            },
            {
                "Sensitivity": {"S": "ALL"},
                "SK": {"S": "PER#2"},
                "Id": {"S": "2"},
                "PK": {"S": "PER#WRITE"},
                "Type": {"S": "WRITE"},
            },
            {
                "Sensitivity": {"S": "PRIVATE"},
                "SK": {"S": "PER#3"},
                "Id": {"S": "3"},
                "PK": {"S": "PER#READ"},
                "Type": {"S": "READ"},
            },
        ],
        "Count": 4,
        "ScannedCount": 5,
    }
    permissions_list = [
        PermissionItem(perm_id="0", sensitivity=None, perm_type="USER_ADMIN"),
        PermissionItem(perm_id="1", sensitivity="ALL", perm_type="READ"),
        PermissionItem(perm_id="2", sensitivity="ALL", perm_type="WRITE"),
        PermissionItem(perm_id="3", sensitivity="PRIVATE", perm_type="READ"),
    ]

    def setup_method(self):
        self.dynamo_boto_client = Mock()
        self.test_permissions_table_name = "TEST PERMISSIONS"
        self.dynamo_adapter = DynamoDBAdapter(
            self.dynamo_boto_client, self.test_permissions_table_name
        )

    def test_dynamo_db_create_client_item(self):
        client_id = "123456789"
        client_permissions = ["READ_ALL", "WRITE_ALL"]
        self.dynamo_boto_client.scan.return_value = self.expected_db_scan_response
        self.dynamo_adapter.create_client_item(client_id, client_permissions)

        self.dynamo_boto_client.put_item.assert_called_once_with(
            TableName=self.test_permissions_table_name,
            Item={
                "PK": {"S": "USR#CLIENT"},
                "SK": {"S": f"USR#${client_id}"},
                "Id": {"S": f"${client_id}"},
                "Type": {"S": "CLIENT"},
                "Permissions": {"SS": ["1", "2"]},
            },
        )

    def test_dynamo_db_create_client_item_throws_error(self):
        client_id = "123456789"
        client_permissions = ["READ_ALL", "WRITE_ALL"]
        self.dynamo_boto_client.scan.return_value = self.expected_db_scan_response

        self.dynamo_boto_client.put_item.side_effect = ClientError(
            error_response={"Error": {"Code": "ConditionalCheckFailedException"}},
            operation_name="PutItem",
        )

        with pytest.raises(
            AWSServiceError,
            match="The client could not be created, please contact system administrator",
        ):
            self.dynamo_adapter.create_client_item(client_id, client_permissions)

    def test_dynamo_db_create_subject_throws_error(self):
        subject_id = '123456789'
        permission_ids = ["0", "1"]
        subject_type = 'CLIENT'

        self.dynamo_boto_client.put_item.side_effect = ClientError(
            error_response={"Error": {"Code": "ConditionalCheckFailedException"}},
            operation_name="PutItem",
        )

        with pytest.raises(
                AWSServiceError, match="The client could not be created, please contact system administrator"
        ):
            self.dynamo_adapter.create_subject_permission(subject_type, subject_id, permission_ids)

    def test_get_db_permissions_for_user_admin(self):
        self.dynamo_boto_client.scan.return_value = {
            "Items": [
                {
                    "SK": {"S": "PER#0"},
                    "Id": {"S": "0"},
                    "PK": {"S": "PER#USER_ADMIN"},
                    "Type": {"S": "USER_ADMIN"},
                }
            ]
        }
        expected_response = [
            PermissionItem(perm_id="0", sensitivity=None, perm_type="USER_ADMIN")
        ]
        response = self.dynamo_adapter.get_db_permissions()

        self.dynamo_boto_client.scan.assert_called_once_with(
            TableName=self.test_permissions_table_name,
            ScanFilter={
                "PK": {
                    "AttributeValueList": [
                        {
                            "S": "PER",
                        },
                    ],
                    "ComparisonOperator": "BEGINS_WITH",
                }
            },
        )

        assert len(response) == 1
        assert response[0].permission == expected_response[0].permission
        assert response[0].id == expected_response[0].id
        assert response[0].sensitivity == expected_response[0].sensitivity
        assert response[0].type == expected_response[0].type

    def test_get_db_permissions(self):
        self.dynamo_boto_client.scan.return_value = {
            "Items": [
                {
                    "Sensitivity": {"S": "ALL"},
                    "SK": {"S": "PER#2"},
                    "Id": {"S": "2"},
                    "PK": {"S": "PER#WRITE"},
                    "Type": {"S": "WRITE"},
                },
                {
                    "Sensitivity": {"S": "PRIVATE"},
                    "SK": {"S": "PER#3"},
                    "Id": {"S": "3"},
                    "PK": {"S": "PER#READ"},
                    "Type": {"S": "READ"},
                },
            ]
        }
        expected_response = [
            PermissionItem(perm_id="2", sensitivity="ALL", perm_type="WRITE"),
            PermissionItem(perm_id="3", sensitivity="PRIVATE", perm_type="READ"),
        ]
        response = self.dynamo_adapter.get_db_permissions()

        self.dynamo_boto_client.scan.assert_called_once_with(
            TableName=self.test_permissions_table_name,
            ScanFilter={
                "PK": {
                    "AttributeValueList": [
                        {
                            "S": "PER",
                        },
                    ],
                    "ComparisonOperator": "BEGINS_WITH",
                }
            },
        )

        assert len(response) == 2
        assert response[0].permission == expected_response[0].permission
        assert response[0].id == expected_response[0].id
        assert response[0].sensitivity == expected_response[0].sensitivity
        assert response[0].type == expected_response[0].type
        assert response[1].permission == expected_response[1].permission
        assert response[1].id == expected_response[1].id
        assert response[1].sensitivity == expected_response[1].sensitivity
        assert response[1].type == expected_response[1].type

    def test_get_db_permissions_handles_aws_error(self):
        self.dynamo_boto_client.scan.side_effect = ClientError(
            error_response={"Error": {"Code": "ResourceNotFoundException"}},
            operation_name="Scan",
        )

        with pytest.raises(
            AWSServiceError,
            match="Internal server error, please contact system administrator",
        ):
            self.dynamo_adapter.get_db_permissions()

    def test_get_existing_permissions_for_user_with_db(self):
        test_user_permissions = ["READ_PRIVATE", "WRITE_ALL"]
        response = self.dynamo_adapter.get_validated_permission_ids(
            self.permissions_list, test_user_permissions
        )
        assert response == ["3", "2"]

    def test_get_existing_permissions_for_user_with_no_permissions_from_db(self):
        test_user_permissions = []
        response = self.dynamo_adapter.get_validated_permission_ids(
            self.permissions_list, test_user_permissions
        )
        assert response == []

    def test_get_existing_permissions_for_user_with_all_invalid_permissions_from_db(
        self,
    ):
        test_user_permissions = ["READ_SENSITIVE", "ACCESS_ALL", "ADMIN", "FAKE_ADMIN"]
        with pytest.raises(
            UserError, match="One or more of the provided permissions do not exist"
        ):
            self.dynamo_adapter.get_validated_permission_ids(
                self.permissions_list, test_user_permissions
            )

    def test_get_existing_permissions_for_user_with_some_invalid_permissions_from_db(
        self,
    ):
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
            self.dynamo_adapter.get_validated_permission_ids(
                self.permissions_list, test_user_permissions
            )

    def test_get_existing_permissions_from_db_with_user_admin_permission(self):
        test_user_permissions = ["USER_ADMIN", "WRITE_ALL"]
        response = self.dynamo_adapter.get_validated_permission_ids(
            self.permissions_list, test_user_permissions
        )
        assert response == ["0", "2"]
