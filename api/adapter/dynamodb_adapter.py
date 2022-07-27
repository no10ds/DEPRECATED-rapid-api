from abc import ABC, abstractmethod
from functools import reduce
from typing import List

import boto3
from boto3.dynamodb.conditions import Key, Attr, Or
from botocore.exceptions import ClientError

from api.common.config.aws import AWS_REGION, DYNAMO_PERMISSIONS_TABLE_NAME
from api.common.custom_exceptions import UserError, AWSServiceError
from api.domain.permission_item import PermissionItem


class DatabaseAdapter(ABC):
    @abstractmethod
    def create_client_item(self, client_id: str, client_permissions: List[str]) -> None:
        pass

    @abstractmethod
    def create_subject_permission(
        self, subject_type: str, subject_id: str, permissions: List[str]
    ) -> None:
        pass

    @abstractmethod
    def validate_permission(self, subject_permissions: List[str]):
        pass

    @abstractmethod
    def get_all_permissions(self) -> List[dict]:
        pass

    @abstractmethod
    def get_permissions_for_subject(self, subject_id: str) -> List[dict]:
        pass


class DynamoDBAdapter(DatabaseAdapter):
    def __init__(
        self,
        dynamodb_table=boto3.resource("dynamodb", region_name=AWS_REGION).Table(
            DYNAMO_PERMISSIONS_TABLE_NAME
        ),
    ):
        self.dynamodb_resource = dynamodb_table

    def create_client_item(self, client_id: str, client_permissions: List[str]):
        self.create_subject_permission(
            subject_type="CLIENT", subject_id=client_id, permissions=client_permissions
        )

    def create_subject_permission(
        self, subject_type: str, subject_id: str, permissions: List[str]
    ):
        try:
            self.dynamodb_resource.put_item(
                Item={
                    "PK": "SUBJECT",
                    "SK": subject_id,
                    "Id": subject_id,
                    "Type": subject_type,
                    "Permissions": set(permissions),
                },
            )
        except ClientError:
            raise AWSServiceError(
                "The client could not be created, please contact your system administrator"
            )

    def validate_permission(self, subject_permissions: List[str]):
        permissions_response = self._get_permissions(subject_permissions)
        if not permissions_response["Count"] == len(subject_permissions):
            raise UserError("One or more of the provided permissions do not exist")

    def get_all_permissions(self) -> List[PermissionItem]:
        raise NotImplementedError()

    def get_permissions_for_subject(self, subject_id: str) -> List[str]:
        try:
            return list(
                self.dynamodb_resource.query(
                    KeyConditionExpression=Key("PK").eq("SUBJECT"),
                    FilterExpression=Attr("Id").eq(subject_id),
                )["Items"][0]["Permissions"]
            )
        except ClientError:
            raise AWSServiceError(
                "Error fetching permissions, please contact your system administrator"
            )

    def _get_permissions(self, subject_permissions):
        try:
            return self.dynamodb_resource.query(
                KeyConditionExpression=Key("PK").eq("PERMISSION"),
                FilterExpression=reduce(
                    Or, ([Attr("Id").eq(value) for value in subject_permissions])
                ),
            )
        except ClientError:
            raise AWSServiceError(
                "The client could not be created, please contact your system administrator"
            )
