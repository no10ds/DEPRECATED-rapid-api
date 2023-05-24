from itertools import compress
from unittest.mock import Mock
import pytest

from api.application.services.authorisation.dataset_access_evaluator import (
    DatasetAccessEvaluator,
    LayerPermissionConverter,
)
from api.common.config.auth import Action
from api.common.custom_exceptions import AuthorisationError
from api.domain.dataset_filters import DatasetFilters
from api.domain.dataset_metadata import DatasetMetadata
from api.domain.permission_item import PermissionItem
from api.domain.schema_metadata import Tags


class TestDatasetAccessEvaluator:
    def setup_method(self):
        self.glue_adapter = Mock()
        self.resource_adapter = Mock()
        self.permission_service = Mock()
        self.evaluator = DatasetAccessEvaluator(
            glue_adapter=self.glue_adapter,
            resource_adapter=self.resource_adapter,
            permission_service=self.permission_service,
        )

    def test_layer_permission_converter(self):
        expected = {"RAW": ["raw"], "LAYER": ["layer"], "ALL": ["raw", "layer"]}
        actual = {i.name: i.value for i in LayerPermissionConverter}
        assert actual == expected

    @pytest.mark.parametrize(
        "permission, expected_filters",
        [
            (
                PermissionItem(
                    id="READ_ALL_ALL", layer="ALL", sensitivity="ALL", type="READ"
                ),
                DatasetFilters(
                    sensitivity=["PUBLIC", "PRIVATE", "PROTECTED"],
                    layer=["raw", "layer"],
                ),
            ),
            (
                PermissionItem(
                    id="WRITE_RAW_ALL",
                    layer="RAW",
                    sensitivity="ALL",
                    type="WRITE",
                ),
                DatasetFilters(
                    sensitivity=["PUBLIC", "PRIVATE", "PROTECTED"], layer=["raw"]
                ),
            ),
            (
                PermissionItem(
                    id="WRITE_ALL_PUBLIC",
                    layer="ALL",
                    sensitivity="PUBLIC",
                    type="WRITE",
                ),
                DatasetFilters(sensitivity=["PUBLIC"], layer=["raw", "layer"]),
            ),
            (
                PermissionItem(
                    id="READ_RAW_PROTECTED_TEST",
                    layer="RAW",
                    sensitivity="PROTECTED",
                    type="READ",
                    domain="TEST",
                ),
                DatasetFilters(sensitivity=["PROTECTED"], layer=["raw"], domain="TEST"),
            ),
        ],
    )
    def test_extract_datasets_from_permission(self, permission, expected_filters):
        self.resource_adapter.get_datasets_metadata = Mock()
        self.resource_adapter.get_datasets_metadata.return_value = "dataset"

        res = self.evaluator.extract_datasets_from_permission(permission)

        self.resource_adapter.get_datasets_metadata.assert_called_once_with(
            expected_filters
        )
        assert res == "dataset"

    def test_fetch_datasets(self):
        permissions = [
            PermissionItem(
                id="READ_ALL",
                layer="ALL",
                sensitivity="ALL",
                type="READ",
            ),
            PermissionItem(
                id="READ_RAW_PROTECTED_TEST",
                layer="RAW",
                sensitivity="PROTECTED",
                type="READ",
                domain="TEST",
            ),
        ]

        self.evaluator.extract_datasets_from_permission = Mock(
            side_effect=[
                [
                    DatasetMetadata(layer="raw", domain="domain", dataset="dataset"),
                    DatasetMetadata(layer="raw", domain="domain_1", dataset="dataset"),
                ],
                [
                    DatasetMetadata(layer="raw", domain="domain", dataset="dataset"),
                    DatasetMetadata(layer="raw", domain="domain_2", dataset="dataset"),
                ],
            ]
        )

        expected = [
            DatasetMetadata(
                layer="raw",
                domain="domain",
                dataset="dataset",
                version=None,
            ),
            DatasetMetadata(
                layer="raw",
                domain="domain_1",
                dataset="dataset",
                version=None,
            ),
            DatasetMetadata(
                layer="raw",
                domain="domain_2",
                dataset="dataset",
                version=None,
            ),
        ]

        res = self.evaluator.fetch_datasets(permissions)

        assert res == expected

    @pytest.mark.parametrize(
        "tags, permission, expected",
        [
            # 0. Success: All criteria overlap directly
            (
                Tags(
                    sensitivity="PUBLIC",
                    no_of_versions=0,
                    layer="raw",
                    domain="test",
                ),
                PermissionItem(
                    id="READ_RAW_PUBLIC", type="READ", layer="RAW", sensitivity="PUBLIC"
                ),
                True,
            ),
            # 1. Success: All criteria overlap directly with protected domain
            (
                Tags(
                    sensitivity="PROTECTED",
                    no_of_versions=0,
                    layer="raw",
                    domain="test",
                ),
                PermissionItem(
                    id="WRITE_RAW_PROTECTED_TEST",
                    type="WRITE",
                    layer="RAW",
                    sensitivity="PROTECTED",
                    domain="test",
                ),
                True,
            ),
            # 2. Success: All criteria inherit overlaps
            (
                Tags(
                    sensitivity="PUBLIC", no_of_versions=0, layer="raw", domain="test"
                ),
                PermissionItem(
                    id="WRITE_ALL_PRIVATE",
                    type="WRITE",
                    layer="ALL",
                    sensitivity="PRIVATE",
                ),
                True,
            ),
            # 3. Failure: Sensitivity does not overlap
            (
                Tags(
                    sensitivity="PRIVATE",
                    no_of_versions=0,
                    layer="raw",
                    domain="test",
                ),
                PermissionItem(
                    id="READ_RAW_PUBLIC", type="READ", layer="RAW", sensitivity="PUBLIC"
                ),
                False,
            ),
            # 4. Failure: Layer does not overlap
            (
                Tags(
                    sensitivity="PRIVATE",
                    no_of_versions=0,
                    layer="raw",
                    domain="test",
                ),
                PermissionItem(
                    id="READ_LAYER_PRIVATE",
                    type="READ",
                    layer="LAYER",
                    sensitivity="PRIVATE",
                ),
                False,
            ),
            # 5. Failure: Is protected and domain does not overlap
            (
                Tags(
                    sensitivity="PROTECTED",
                    no_of_versions=0,
                    layer="raw",
                    domain="test_fail",
                ),
                PermissionItem(
                    id="READ_ANY_PROTECTED_TEST",
                    type="READ",
                    layer="ALL",
                    sensitivity="PROTECTED",
                    domain="TEST",
                ),
                False,
            ),
        ],
    )
    def test_tags_overlap_with_permission(
        self, tags: Tags, permission: PermissionItem, expected: bool
    ):
        res = self.evaluator.tags_overlap_with_permission(tags, permission)
        assert res == expected

    @pytest.mark.parametrize(
        "action, permission_mask",
        [(Action.READ, [True, False, False]), (Action.WRITE, [False, True, True])],
    )
    def test_get_authorised_datasets(self, action: Action, permission_mask: list[bool]):
        subject_id = "abc-123"
        permissions = [
            PermissionItem(
                id="READ_ALL_PUBLIC", layer="ALL", type="READ", sensitivity="PUBLIC"
            ),
            PermissionItem(
                id="WRITE_ALL_PUBLIC", layer="ALL", type="WRITE", sensitivity="PUBLIC"
            ),
            PermissionItem(
                id="WRITE_ALL_PRIVATE", layer="ALL", type="WRITE", sensitivity="PRIVATE"
            ),
        ]

        self.permission_service.get_subject_permissions = Mock(return_value=permissions)
        self.evaluator.fetch_datasets = Mock(return_value=["dataset"])

        res = self.evaluator.get_authorised_datasets(subject_id, action)
        assert res == ["dataset"]
        self.permission_service.get_subject_permissions.assert_called_once_with(
            subject_id
        )
        self.evaluator.fetch_datasets.assert_called_once_with(
            list(compress(permissions, permission_mask))
        )

    def test_can_access_dataset_success(self):
        subject_id = "abc-123"
        dataset = "dataset"
        read_permissions = [
            PermissionItem(
                id="READ_ALL_PUBLIC", layer="ALL", type="READ", sensitivity="PUBLIC"
            )
        ]
        write_permissions = [
            PermissionItem(
                id="WRITE_ALL_PUBLIC", layer="ALL", type="WRITE", sensitivity="PUBLIC"
            ),
            PermissionItem(
                id="WRITE_ALL_PRIVATE", layer="ALL", type="WRITE", sensitivity="PRIVATE"
            ),
        ]
        tags = Tags(
            sensitivity="PRIVATE",
            no_of_versions=0,
            layer="raw",
            domain="test_fail",
        )

        self.permission_service.get_subject_permissions = Mock(
            return_value=read_permissions + write_permissions
        )
        self.glue_adapter.get_crawler_tags = Mock(return_value=tags)

        res = self.evaluator.can_access_dataset(
            dataset, subject_id, [Action.WRITE, Action.READ]
        )

        self.permission_service.get_subject_permissions.assert_called_once_with(
            subject_id
        )
        self.glue_adapter.get_crawler_tags.assert_called_once_with(dataset)
        assert res is True

    def test_can_access_dataset_failure(self):
        subject_id = "abc-123"
        dataset = DatasetMetadata("layer", "domain", "dataset")
        read_permissions = [
            PermissionItem(
                id="READ_ALL_PUBLIC", layer="ALL", type="READ", sensitivity="PUBLIC"
            )
        ]
        write_permissions = [
            PermissionItem(
                id="WRITE_ALL_PUBLIC", layer="ALL", type="WRITE", sensitivity="PUBLIC"
            ),
            PermissionItem(
                id="WRITE_ALL_PRIVATE", layer="ALL", type="WRITE", sensitivity="PRIVATE"
            ),
        ]
        tags = Tags(
            sensitivity="PRIVATE",
            no_of_versions=0,
            layer="raw",
            domain="test_fail",
        )

        self.permission_service.get_subject_permissions = Mock(
            return_value=read_permissions + write_permissions
        )
        self.glue_adapter.get_crawler_tags = Mock(return_value=tags)

        with pytest.raises(
            AuthorisationError,
            match="User abc-123 does not have enough permisisons to access the dataset layer \\[layer\\], domain \\[domain\\] and dataset \\[dataset\\]",
        ):
            self.evaluator.can_access_dataset(dataset, subject_id, [Action.READ])

            self.permission_service.get_subject_permissions.assert_called_once_with(
                subject_id
            )
            self.glue_adapter.get_crawler_tags.assert_called_once_with(dataset)
