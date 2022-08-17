from typing import Set, Dict, List

from api.adapter.aws_resource_adapter import AWSResourceAdapter
from api.adapter.dynamodb_adapter import DynamoDBAdapter
from api.common.config.auth import SensitivityLevel, Action
from api.domain.dataset_filters import DatasetFilters

WRITE_ALL = f"{Action.WRITE.value}_ALL"
WRITE_PRIVATE = f"{Action.WRITE.value}_{SensitivityLevel.PRIVATE.value}"
WRITE_PUBLIC = f"{Action.WRITE.value}_{SensitivityLevel.PUBLIC.value}"
READ_ALL = f"{Action.READ.value}_ALL"
READ_PRIVATE = f"{Action.READ.value}_{SensitivityLevel.PRIVATE.value}"
READ_PUBLIC = f"{Action.READ.value}_{SensitivityLevel.PUBLIC.value}"

sensitivities_dict = {
    WRITE_ALL: SensitivityLevel.get_all_values(),
    WRITE_PRIVATE: [SensitivityLevel.PRIVATE.value, SensitivityLevel.PUBLIC.value],
    WRITE_PUBLIC: [SensitivityLevel.PUBLIC.value],
    READ_ALL: SensitivityLevel.get_all_values(),
    READ_PRIVATE: [SensitivityLevel.PRIVATE.value, SensitivityLevel.PUBLIC.value],
    READ_PUBLIC: [SensitivityLevel.PUBLIC.value],
}


class DatasetService:
    def __init__(
        self, dynamodb_adapter=DynamoDBAdapter(), resource_adapter=AWSResourceAdapter()
    ):
        self.dynamodb_adapter = dynamodb_adapter
        self.resource_adapter = resource_adapter

    def get_authorised_datasets(self, subject_id: str, action: Action) -> List[str]:
        permissions = self.dynamodb_adapter.get_permissions_for_subject(subject_id)
        sensitivities_and_domains = self._extract_sensitivities_and_domains(
            permissions, action
        )
        return self._fetch_datasets(sensitivities_and_domains)

    def _extract_sensitivities_and_domains(
        self, permissions: List[str], action: Action
    ) -> Dict[str, Set[str]]:
        sensitivities = set()
        protected_domains = set()

        relevant_permissions = [
            permission
            for permission in permissions
            if permission.startswith(action.value)
        ]
        for permission in relevant_permissions:
            if self._is_protected_permission(permission, action):
                slice_index = self._protected_index_map(action)
                protected_domains.add(permission[slice_index:])
            else:
                sensitivities.update(sensitivities_dict.get(permission))
        return {"protected_domains": protected_domains, "sensitivities": sensitivities}

    def _fetch_datasets(self, sensitivities_and_domains: Dict[str, Set[str]]):
        authorised_datasets = set()
        if len(sensitivities_and_domains.get("sensitivities")) > 0:
            self._extract_datasets_from_sensitivities(
                authorised_datasets, sensitivities_and_domains
            )
        if len(sensitivities_and_domains.get("protected_domains")) > 0:
            self._extract_datasets_from_protected_domains(
                authorised_datasets, sensitivities_and_domains
            )

        return sorted(authorised_datasets)

    def _extract_datasets_from_protected_domains(
        self, authorised_datasets, sensitivities_and_domains
    ):
        query = DatasetFilters(sensitivity=SensitivityLevel.PROTECTED.value)
        datasets_metadata_list_protected_domains = (
            self.resource_adapter.get_datasets_metadata(query)
        )
        for protected_domain in sensitivities_and_domains.get("protected_domains"):
            [
                authorised_datasets.add(dataset.get_ui_upload_path())
                for dataset in datasets_metadata_list_protected_domains
                if dataset.domain == protected_domain.lower()
            ]

    def _extract_datasets_from_sensitivities(
        self, authorised_datasets, sensitivities_and_domains
    ):
        datasets_metadata_list_sensitivities = []

        for sensitivity in sensitivities_and_domains.get("sensitivities"):
            query = DatasetFilters(sensitivity=sensitivity)
            datasets_metadata_list_sensitivities.extend(
                self.resource_adapter.get_datasets_metadata(query)
            )
        return [
            authorised_datasets.add(datasets_metadata.get_ui_upload_path())
            for datasets_metadata in datasets_metadata_list_sensitivities
        ]

    def _is_protected_permission(self, permission: str, action: Action) -> bool:
        return permission.startswith(
            f"{action.value}_{SensitivityLevel.PROTECTED.value}_"
        )

    def _protected_index_map(self, action: Action) -> int:
        return {
            Action.WRITE.value: len(
                f"{Action.WRITE.value}_{SensitivityLevel.PROTECTED.value}_"
            ),
            Action.READ.value: len(
                f"{Action.READ.value}_{SensitivityLevel.PROTECTED.value}_"
            ),
        }[action.value]