from abc import ABC, abstractmethod
from functools import reduce
from typing import List, Dict, Any

import boto3
from boto3.dynamodb.conditions import Key, Attr, Or
from botocore.exceptions import ClientError

from api.common.config.auth import DatabaseItem, SubjectType
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
        try:
            self.dynamodb_table.put_item(
                Item={
                    "PK": DatabaseItem.SUBJECT.value,
                    "SK": subject_id,
                    "Id": subject_id,
                    "Type": subject_type.value,
                    "Permissions": set(permissions),
                },
            )
        except ClientError:
            self._handle_client_error(
                f"Error storing the {subject_type.value}: {subject_id}"
            )

    def validate_permissions(self, subject_permissions: List[str]) -> None:
        permissions_response = self._get_permissions(subject_permissions)
        if not permissions_response["Count"] == len(subject_permissions):
            AppLogger.info(f"Invalid permission in {subject_permissions}")
            raise UserError(
                "One or more of the provided permissions is invalid or duplicated"
            )

    def get_all_permissions(self) -> List[PermissionItem]:
        raise NotImplementedError()

    def get_permissions_for_subject(self, subject_id: str) -> List[str]:
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

    def _get_permissions(self, permissions: List[str]) -> Dict[str, Any]:
        try:
            return self.dynamodb_table.query(
                KeyConditionExpression=Key("PK").eq(DatabaseItem.PERMISSION.value),
                FilterExpression=reduce(
                    Or, ([Attr("Id").eq(value) for value in permissions])
                ),
            )
        except ClientError:
            self._handle_client_error("Error fetching permissions from the database")

    def update_subject_permissions(
        self, subject_permissions: SubjectPermissions
    ) -> None:
        try:
            self.dynamodb_table.update_item(
                Key={
                    "PK": DatabaseItem.SUBJECT.value,
                    "SK": subject_permissions.subject_id,
                },
                UpdateExpression="set #P = :r",
                ExpressionAttributeValues={":r": set(subject_permissions.permissions)},
                ExpressionAttributeNames={"#P": "Permissions"},
            )
        except ClientError:
            self._handle_client_error(
                f"Error updating permissions for {subject_permissions.subject_id}"
            )

    @staticmethod
    def _handle_client_error(message: str) -> None:
        AppLogger.error(message)
        raise AWSServiceError(message)
