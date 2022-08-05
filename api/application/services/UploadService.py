from typing import Set

from api.adapter.aws_resource_adapter import AWSResourceAdapter
from api.adapter.dynamodb_adapter import DynamoDBAdapter
from api.domain.dataset_filters import DatasetFilters


class UploadService:
    def __init__(
        self, dynamodb_adapter=DynamoDBAdapter(), resource_adapter=AWSResourceAdapter()
    ):
        self.dynamodb_adapter = dynamodb_adapter
        self.resource_adapter = resource_adapter

    def get_authorised_datasets(self, subject_id: str) -> Set[str]:
        start_index = len("WRITE_")
        permissions = self.dynamodb_adapter.get_permissions_for_subject(subject_id)
        sensitivities_and_domains = [
            permission[start_index:]
            for permission in permissions
            if permission.startswith("WRITE")
        ]
        authorised_datasets = set()
        datasets_metadata_list = []
        for sensitivity in sensitivities_and_domains:
            query = DatasetFilters(sensitivity=sensitivity)
            datasets_metadata_list.extend(
                self.resource_adapter.get_datasets_metadata(query)
            )

        for datasets_metadata in datasets_metadata_list:
            authorised_datasets.add(datasets_metadata.dataset)
        return authorised_datasets
