from enum import Enum
from typing import List

from api.common.custom_exceptions import AuthorisationError
from api.common.config.auth import Action, Sensitivity, ALL, Layer
from api.adapter.aws_resource_adapter import AWSResourceAdapter
from api.adapter.glue_adapter import GlueAdapter
from api.application.services.permissions_service import PermissionsService
from api.domain.dataset_filters import DatasetFilters
from api.domain.schema_metadata import Tags
from api.domain.dataset_metadata import DatasetMetadata
from api.domain.permission_item import PermissionItem


class SensitivityPermissionConverter(Enum):
    ALL = list(Sensitivity)
    PRIVATE = [Sensitivity.PRIVATE, Sensitivity.PUBLIC]
    PUBLIC = [Sensitivity.PUBLIC]
    PROTECTED = [Sensitivity.PROTECTED]


LayerPermissionConverter = Enum(
    "LayerPermissionConverter",
    dict([(layer.upper(), [layer]) for layer in list(Layer)] + [(ALL, list(Layer))]),
)


class DatasetAccessEvaluator:
    def __init__(
        self,
        glue_adapter=GlueAdapter(),
        resource_adapter=AWSResourceAdapter(),
        permission_service=PermissionsService(),
    ):
        self.glue_adapter = glue_adapter
        self.resource_adapter = resource_adapter
        self.permission_serivice = permission_service

    def get_authorised_datasets(
        self, subject_id: str, action: Action
    ) -> List[DatasetMetadata]:
        """
        This function does the following:
        1. Get the permissions of the subject
        2. Filters the permission by the relevant action e.g READ/WRITE
        3. Queries the datasets to find those that match these permissions
        4. Returns them
        """
        permissions = self.permission_serivice.get_subject_permissions(subject_id)
        permissions = self.filter_permissions_by_action(permissions, action)
        return self.fetch_datasets(permissions)

    def can_access_dataset(
        self, dataset: DatasetMetadata, subject_id: str, actions: List[Action]
    ):
        """
        This function does the following:
        1. Gets the permisisons of the subject
        2. Gets the tags of the dataset
        3. Loops through the dataset actions
        4. Filters the permission by the relevant action
        5. Assesses if the tags overlap with the permisisons, returning True if they do
        6. Raise Authorisation if the loop is over and there was no permission overlap
        """
        permissions = self.permission_serivice.get_subject_permissions(subject_id)
        tags = self.glue_adapter.get_crawler_tags(dataset)

        for action in actions:
            filtered_permissions = self.filter_permissions_by_action(
                permissions, action
            )
            if any(
                self.tags_overlap_with_permission(tags, permission)
                for permission in filtered_permissions
            ):
                return True
        raise AuthorisationError(
            f"User {subject_id} does not have enough permisisons to access the dataset {dataset.string_representation()}"
        )

    def tags_overlap_with_permission(
        self, tags: Tags, permission: PermissionItem
    ) -> bool:
        return all(
            [
                tags.sensitivity
                in SensitivityPermissionConverter[permission.sensitivity].value,
                tags.layer in LayerPermissionConverter[permission.layer].value,
                tags.domain == permission.domain
                if permission.is_protected_permission()
                else True,
            ]
        )

    def filter_permissions_by_action(
        self, permissions: List[PermissionItem], action: Action
    ):
        return [permission for permission in permissions if permission.type == action]

    def fetch_datasets(
        self, permissions: List[PermissionItem]
    ) -> List[DatasetMetadata]:
        authorised_datasets = set()
        for permission in permissions:
            authorised_datasets.update(
                self.extract_datasets_from_permission(permission)
            )
        return sorted(authorised_datasets)

    def extract_datasets_from_permission(
        self, permission: PermissionItem
    ) -> List[DatasetMetadata]:
        query = DatasetFilters(
            sensitivity=SensitivityPermissionConverter[permission.sensitivity].value,
            layer=LayerPermissionConverter[permission.layer].value,
            domain=permission.domain,
        )
        return self.resource_adapter.get_datasets_metadata(query)
