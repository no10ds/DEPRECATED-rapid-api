from typing import Set, Dict, List

from api.adapter.aws_resource_adapter import AWSResourceAdapter
from api.adapter.dynamodb_adapter import DynamoDBAdapter
from api.common.config.auth import SensitivityLevel, Action
from api.domain.dataset_filters import DatasetFilters

START_INDEX_WRITE = len(Action.WRITE.value + "_")
START_INDEX_PROTECTED = len(
    Action.WRITE.value + "_" + SensitivityLevel.PROTECTED.value + "_"
)

WRITE_ALL = Action.WRITE.value + "_ALL"
WRITE_PRIVATE = Action.WRITE.value + "_" + SensitivityLevel.PRIVATE.value
WRITE_PUBLIC = Action.WRITE.value + "_" + SensitivityLevel.PUBLIC.value
WRITE_PROTECTED_DOMAIN = (
    Action.WRITE.value + "_" + SensitivityLevel.PROTECTED.value + "_"
)

sensitivities_dict = {
    WRITE_ALL: SensitivityLevel.get_all_values(),
    WRITE_PRIVATE: [SensitivityLevel.PRIVATE.value, SensitivityLevel.PUBLIC.value],
    WRITE_PUBLIC: [SensitivityLevel.PUBLIC.value],
}


class UploadService:
    def __init__(
        self, dynamodb_adapter=DynamoDBAdapter(), resource_adapter=AWSResourceAdapter()
    ):
        self.dynamodb_adapter = dynamodb_adapter
        self.resource_adapter = resource_adapter

    def get_authorised_datasets(self, subject_id: str) -> Set[str]:
        permissions = self.dynamodb_adapter.get_permissions_for_subject(subject_id)
        sensitivities_and_domains = self._extract_sensitivities_and_domains(permissions)
        return self._fetch_datasets(sensitivities_and_domains)

    def _extract_sensitivities_and_domains(
        self, permissions: List[str]
    ) -> Dict[str, Set[str]]:
        sensitivities = set()
        protected_domains = set()

        write_permissions = [
            permission
            for permission in permissions
            if permission.startswith(Action.WRITE.value)
        ]
        for permission in write_permissions:
            if self._is_protected_permission(permission):
                protected_domains.add(permission[START_INDEX_PROTECTED:])
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

        return authorised_datasets

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

    @staticmethod
    def _is_protected_permission(permission) -> bool:
        return permission.startswith(WRITE_PROTECTED_DOMAIN)
