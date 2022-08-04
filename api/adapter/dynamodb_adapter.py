from abc import ABC, abstractmethod
from functools import reduce
from typing import List, Dict, Any

import boto3
from boto3.dynamodb.conditions import Key, Attr, Or
from botocore.exceptions import ClientError

from api.common.config.auth import DatabaseItem, SubjectType, SensitivityLevel
from api.common.config.aws import AWS_REGION, DYNAMO_PERMISSIONS_TABLE_NAME
from api.common.custom_exceptions import (
    UserError,
    AWSServiceError,
    SubjectNotFoundError,
)
from api.common.logger import AppLogger
from api.domain.permission_item import PermissionItem
from api.domain.subject_permissions import SubjectPermissions


class DatabaseAdapter(ABC):
    @abstractmethod
    def store_subject_permissions(
        self, subject_type: SubjectType, subject_id: str, permissions: List[str]
    ) -> None:
        pass

    def store_protected_permission(
        self, permissions: List[PermissionItem], domain: str
    ) -> None:
        pass

    @abstractmethod
    def validate_permissions(self, subject_permissions: List[str]):
        pass

    @abstractmethod
    def get_all_permissions(self) -> List[dict]:
        pass

    @abstractmethod
    def get_permissions_for_subject(self, subject_id: str) -> List[str]:
        pass

    @abstractmethod
    def update_subject_permissions(
        self, subject_permissions: SubjectPermissions
    ) -> None:
        pass


class DynamoDBAdapter(DatabaseAdapter):
    def __init__(
        self,
        dynamodb_table=boto3.resource("dynamodb", region_name=AWS_REGION).Table(
            DYNAMO_PERMISSIONS_TABLE_NAME
        ),
    ):
        self.dynamodb_table = dynamodb_table

    def store_subject_permissions(
        self, subject_type: SubjectType, subject_id: str, permissions: List[str]
    ) -> None:
        subject_type = subject_type.value
        try:
            AppLogger.info(f"Storing permissions for {subject_type}: {subject_id}")
            self.dynamodb_table.put_item(
                Item={
                    "PK": DatabaseItem.SUBJECT.value,
                    "SK": subject_id,
                    "Id": subject_id,
                    "Type": subject_type,
                    "Permissions": set(permissions),
                },
            )
        except ClientError:
            self._handle_client_error(f"Error storing the {subject_type}: {subject_id}")

    def store_protected_permission(
        self, permissions: List[PermissionItem], domain: str
    ) -> None:
        try:
            AppLogger.info(f"Storing protected permissions for {domain}")
            with self.dynamodb_table.batch_writer() as batch:
                for permission in permissions:
                    batch.put_item(
                        Item={
                            "PK": DatabaseItem.PERMISSION.value,
                            "SK": permission.id,
                            "Id": permission.id,
                            "Type": permission.type,
                            "Sensitivity": permission.sensitivity,
                            "Domain": permission.domain,
                        }
                    )
        except ClientError:
            self._handle_client_error(
                f"Error storing the protected domain permission for {domain}"
            )

    def validate_permissions(self, subject_permissions: List[str]) -> None:
        permissions_response = self._find_permissions(subject_permissions)
        if not permissions_response["Count"] == len(subject_permissions):
            AppLogger.info(f"Invalid permission in {subject_permissions}")
            raise UserError(
                "One or more of the provided permissions is invalid or duplicated"
            )

    def get_all_permissions(self) -> List[str]:
        try:
            permissions = self.dynamodb_table.query(
                KeyConditionExpression=Key("PK").eq(DatabaseItem.PERMISSION.value),
            )
            return [permission["SK"] for permission in permissions["Items"]]

        except ClientError as error:
            AppLogger.info(f"Error retrieving all permissions: {error}")
            raise AWSServiceError(
                "Error fetching permissions, please contact your system administrator"
            )

    def get_all_protected_permissions(self) -> List[PermissionItem]:
        list_of_items = self.dynamodb_table.query(
            KeyConditionExpression=Key("PK").eq(DatabaseItem.PERMISSION.value),
            FilterExpression=Attr("Sensitivity").eq(SensitivityLevel.PROTECTED.value),
        )["Items"]
        return [
            self._generate_protected_permission_item(item) for item in list_of_items
        ]

    def get_permissions_for_subject(self, subject_id: str) -> List[str]:
        AppLogger.info(f"Getting permissions for: {subject_id}")
        try:
            return list(
                self.dynamodb_table.query(
                    KeyConditionExpression=Key("PK").eq(DatabaseItem.SUBJECT.value),
                    FilterExpression=Attr("Id").eq(subject_id),
                )["Items"][0]["Permissions"]
            )
        except ClientError:
            AppLogger.info(f"Error retrieving permissions for subject {subject_id}")
            raise AWSServiceError(
                "Error fetching permissions, please contact your system administrator"
            )
        except IndexError:
            AppLogger.info(f"Subject {subject_id} not found")
            raise SubjectNotFoundError("Subject not found in database")

    def update_subject_permissions(
        self, subject_permissions: SubjectPermissions
    ) -> SubjectPermissions:
        try:
            unique_permissions = set(subject_permissions.permissions)
            self.dynamodb_table.update_item(
                Key={
                    "PK": DatabaseItem.SUBJECT.value,
                    "SK": subject_permissions.subject_id,
                },
                ConditionExpression="SK = :sid",
                UpdateExpression="set #P = :r",
                ExpressionAttributeNames={"#P": "Permissions"},
                ExpressionAttributeValues={
                    ":r": unique_permissions,
                    ":sid": subject_permissions.subject_id,
                },
            )
            return SubjectPermissions(
                subject_id=subject_permissions.subject_id,
                permissions=list(unique_permissions),
            )
        except ClientError as error:
            if self._failed_conditions(error):
                message = (
                    f"Subject with ID {subject_permissions.subject_id} does not exist"
                )
                AppLogger.error(message)
                raise UserError(message)
            self._handle_client_error(
                f"Error updating permissions for {subject_permissions.subject_id}"
            )

    def _find_permissions(self, permissions: List[str]) -> Dict[str, Any]:
        try:
            return self.dynamodb_table.query(
                KeyConditionExpression=Key("PK").eq(DatabaseItem.PERMISSION.value),
                FilterExpression=reduce(
                    Or, ([Attr("Id").eq(value) for value in permissions])
                ),
            )
        except ClientError:
            self._handle_client_error("Error fetching permissions from the database")

    def _failed_conditions(self, error):
        return (
            error.response.get("Error").get("Code") == "ConditionalCheckFailedException"
        )

    @staticmethod
    def _handle_client_error(message: str) -> None:
        AppLogger.error(message)
        raise AWSServiceError(message)

    def _generate_protected_permission_item(self, item: dict) -> PermissionItem:
        return PermissionItem(
            id=item["Id"],
            sensitivity=item["Sensitivity"],
            type=item["Type"],
            domain=item["Domain"],
        )
