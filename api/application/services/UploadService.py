from typing import Set, Dict

from api.adapter.aws_resource_adapter import AWSResourceAdapter
from api.adapter.dynamodb_adapter import DynamoDBAdapter
from api.common.config.auth import SensitivityLevel, Action
from api.domain.dataset_filters import DatasetFilters


class UploadService:
    def __init__(
        self, dynamodb_adapter=DynamoDBAdapter(), resource_adapter=AWSResourceAdapter()
    ):
        self.dynamodb_adapter = dynamodb_adapter
        self.resource_adapter = resource_adapter

    def get_authorised_datasets(self, subject_id: str) -> Set[str]:
        sensitivities_and_domains = self._extract_sensitivities_and_domains(subject_id)
        return self._fetch_datasets(sensitivities_and_domains)

    def _fetch_datasets(self, sensitivities_and_domains: Dict[str, Set[str]]):
        authorised_datasets = set()
        datasets_metadata_list = []
        for sensitivity in sensitivities_and_domains.get("sensitivities"):
            query = DatasetFilters(sensitivity=sensitivity)
            datasets_metadata_list.extend(
                self.resource_adapter.get_datasets_metadata(query)
            )
        # for protected_domain in sensitivities_and_domains.get("protected_domains"):
        #     query = DatasetFilters(sensitivity=SensitivityLevel.PROTECTED.value)
        #     datasets_metadata_list.extend(
        #         self.resource_adapter.get_datasets_metadata(query)
        #     )

        [
            authorised_datasets.add(datasets_metadata.dataset)
            for datasets_metadata in datasets_metadata_list
            if datasets_metadata.tags.get("sensitivity")
            == SensitivityLevel.PROTECTED.value
            and datasets_metadata.domain
            in sensitivities_and_domains.get("protected_domains")
        ]
        return authorised_datasets

    def _extract_sensitivities_and_domains(self, subject_id) -> Dict[str, Set[str]]:
        start_index = len(Action.WRITE.value + "_")
        start_index_for_protected = len(
            Action.WRITE.value + "_" + SensitivityLevel.PROTECTED.value + "_"
        )
        permissions = self.dynamodb_adapter.get_permissions_for_subject(subject_id)
        sensitivities = set()
        protected_domains = set()
        # {"domain": set(), "sensitivities": set()}
        for permission in permissions:
            if permission == Action.WRITE.value + "_ALL":
                sensitivities.update(SensitivityLevel.get_all_values())
                break
            elif (
                permission == Action.WRITE.value + "_" + SensitivityLevel.PRIVATE.value
            ):
                sensitivities.update(
                    [SensitivityLevel.PRIVATE.value, SensitivityLevel.PUBLIC.value]
                )
            elif permission.startswith(
                Action.WRITE.value + "_" + SensitivityLevel.PROTECTED.value + "_"
            ):
                sensitivities.add(SensitivityLevel.PROTECTED.value)
                protected_domains.add(permission[start_index_for_protected:])
            elif permission.startswith(Action.WRITE.value):
                sensitivities.add(permission[start_index:])
        return {"protected_domains": protected_domains, "sensitivities": sensitivities}
