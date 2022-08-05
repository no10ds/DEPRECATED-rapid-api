from typing import Set

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

    def _fetch_datasets(self, sensitivities_and_domains):
        authorised_datasets = set()
        datasets_metadata_list = []
        for sensitivity in sensitivities_and_domains:
            query = DatasetFilters(sensitivity=sensitivity)
            datasets_metadata_list.extend(
                self.resource_adapter.get_datasets_metadata(query)
            )

        [
            authorised_datasets.add(datasets_metadata.dataset)
            for datasets_metadata in datasets_metadata_list
        ]
        return authorised_datasets

    def _extract_sensitivities_and_domains(self, subject_id) -> Set[str]:
        start_index = len(Action.WRITE.value + "_")
        permissions = self.dynamodb_adapter.get_permissions_for_subject(subject_id)
        sensitivities_and_domains = set()
        for permission in permissions:
            if permission == Action.WRITE.value + "_ALL":
                sensitivities_and_domains.update(SensitivityLevel.get_all_values())
            elif (
                permission == Action.WRITE.value + "_" + SensitivityLevel.PRIVATE.value
            ):
                sensitivities_and_domains.update(
                    [SensitivityLevel.PRIVATE.value, SensitivityLevel.PUBLIC.value]
                )
            elif permission.startswith(Action.WRITE.value):
                sensitivities_and_domains.add(permission[start_index:])
        return sensitivities_and_domains
