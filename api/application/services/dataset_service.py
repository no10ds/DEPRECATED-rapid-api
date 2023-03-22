from dataclasses import dataclass
from enum import Enum
import json
from typing import List, Set, Tuple

from api.common.config.auth import Action, LayerPermissions, SensitivityLevel, ALL
from api.adapter.aws_resource_adapter import AWSResourceAdapter
from api.adapter.dynamodb_adapter import DynamoDBAdapter
from api.adapter.s3_adapter import S3Adapter
from api.domain.dataset_filters import DatasetFilters


class SensitivityPermissionConverter(Enum):
    ALL = SensitivityLevel.get_all_values()
    PRIVATE = [SensitivityLevel.PRIVATE.value, SensitivityLevel.PUBLIC.value]
    PUBLIC = [SensitivityLevel.PUBLIC.value]


@dataclass
class Permission:
    layer: LayerPermissions
    scope: str

    def __hash__(self):
        return hash(json.dumps({"layer": self.layer, "scope": self.scope}))


class DatasetService:
    def __init__(
        self,
        dynamodb_adapter=DynamoDBAdapter(),
        resource_adapter=AWSResourceAdapter(),
        s3_adapter=S3Adapter(),
    ):
        self.dynamodb_adapter = dynamodb_adapter
        self.resource_adapter = resource_adapter
        self.s3_adapter = s3_adapter

    def get_authorised_datasets(self, subject_id: str, action: Action) -> List[str]:
        """
        This function does the following
        1. Get subject permissions of the subject
        2. Formats these from strings into objects of the permissions class above
        2. Gets the protected domains and sensitivities of these permissions with the layers
        3. Filters datasets for a. Sensitivity and b. Protected domains
        4. Returns them
        """
        permissions = self.dynamodb_adapter.get_permissions_for_subject(subject_id)
        permissions = self.process_permissions(permissions, action)
        sensitivities, domains = self._split_permissions_into_sensitivies_and_domains(
            permissions
        )
        return self._fetch_datasets(sensitivities, domains)

    def process_permissions(
        self, permissions: List[str], action: Action
    ) -> List[Permission]:
        permissions = self.filter_permissions_by_action(permissions, action)
        return [
            self.transform_permission_from_string(permission)
            for permission in permissions
        ]

    def transform_permission_from_string(self, permission: str) -> Permission:
        for layer in LayerPermissions:
            # Checking for layer with an underscore to protect against two layers with the same prefix
            layer_prefix = f"{layer}_"
            if permission.startswith(layer_prefix):
                return Permission(layer, permission[len(layer_prefix) :])  # noqa: E203
            elif permission == ALL:
                return Permission(ALL, ALL)

    def filter_permissions_by_action(self, permissions: List[str], action: Action):
        return [
            permission[len(action.value) + 1 :]  # noqa: E203
            for permission in permissions
            if permission.startswith(action.value)
        ]

    def _split_permissions_into_sensitivies_and_domains(
        self, permissions: List[Permission]
    ) -> Tuple[Set[Permission], Set[Permission]]:
        sensitivities = set()
        protected_domains = set()

        for permission in permissions:
            if self._is_protected_permission(permission.scope):
                protected_domains.add(permission)
            else:
                sensitivities.add(permission)
        return sensitivities, protected_domains

    def _fetch_datasets(self, sensitivities: Set[Permission], domains: Set[Permission]):
        authorised_datasets = set()
        if len(sensitivities) > 0:
            self._extract_datasets_from_sensitivity_permissions(
                authorised_datasets, sensitivities
            )
        if len(domains) > 0:
            self._extract_datasets_from_protected_domain_permissons(
                authorised_datasets, domains
            )

        return sorted(authorised_datasets)

    def generate_layer_filter(self, permission: Permission):
        """
        Return to filter by layers if the permission is not ALL
        """
        if permission.layer != ALL:
            return {"layer": permission.layer.lower()}
        else:
            return {}

    def _extract_datasets_from_protected_domain_permissons(
        self, authorised_datasets, protected_domain_permissions: Set[Permission]
    ):
        for permission in protected_domain_permissions:
            domain = permission.scope[
                len(f"{SensitivityLevel.PROTECTED.value}_") :  # noqa: E203
            ].lower()
            query = DatasetFilters(
                **self.generate_layer_filter(permission),
                sensitivity=SensitivityLevel.PROTECTED.value,
                domain=domain,
            )
            datasets_metadata_list_protected_domains = (
                self.resource_adapter.get_datasets_metadata(self.s3_adapter, query)
            )

            for dataset in datasets_metadata_list_protected_domains:
                authorised_datasets.add(dataset.get_ui_upload_path())

    def _unpack_sensitivity_permissions(
        self, permissions: Set[Permission]
    ) -> Set[Permission]:
        unpacked_sensitivity_permissions = set()
        for permission in permissions:
            for sensitivity in SensitivityPermissionConverter[permission.scope].value:
                unpacked_sensitivity_permissions.add(
                    Permission(permission.layer, sensitivity)
                )
        return unpacked_sensitivity_permissions

    def _extract_datasets_from_sensitivity_permissions(
        self, authorised_datasets: set, sensitivity_permissions: Set[Permission]
    ):
        datasets_metadata_list_sensitivities = []
        permissions = self._unpack_sensitivity_permissions(sensitivity_permissions)
        for permission in permissions:
            query = DatasetFilters(
                **self.generate_layer_filter(permission),
                sensitivity=permission.scope,
            )
            response = self.resource_adapter.get_datasets_metadata(
                self.s3_adapter, query
            )
            datasets_metadata_list_sensitivities.extend(response)

        return [
            authorised_datasets.add(datasets_metadata.get_ui_upload_path())
            for datasets_metadata in datasets_metadata_list_sensitivities
        ]

    def _is_protected_permission(self, permission: str) -> bool:
        return permission.startswith(f"{SensitivityLevel.PROTECTED.value}_")
