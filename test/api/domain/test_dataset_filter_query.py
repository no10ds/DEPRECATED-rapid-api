import pytest

from api.common.custom_exceptions import UserError
from api.domain.dataset_filters import DatasetFilters

# TODO: Come back to this


# def test_returns_empty_tag_filter_list_when_query_is_empty():
#     query = DatasetFilters()
#     assert query.format_resource_query() == []


# def test_returns_tag_filter_list_when_querying_for_tags_with_values():
#     query = DatasetFilters(
#         key_value_tags={"tag1": "value1", "tag2": "value2"},
#         key_only_tags=["tag3", "tag4"],
#     )

#     expected_tag_filters = [
#         {"Key": "tag1", "Values": ["value1"]},
#         {"Key": "tag2", "Values": ["value2"]},
#         {"Key": "tag3", "Values": []},
#         {"Key": "tag4", "Values": []},
#     ]

#     assert query.format_resource_query() == expected_tag_filters


# def test_returns_tag_filter_list_when_querying_for_tags_without_values():
#     query = DatasetFilters(key_value_tags={"tag1": "", "tag2": None})

#     expected_tag_filters = [
#         {"Key": "tag1", "Values": []},
#         {"Key": "tag2", "Values": []},
#     ]

#     assert query.format_resource_query() == expected_tag_filters


# def test_returns_tag_filter_list_when_querying_for_tags_with_and_without_values():
#     query = DatasetFilters(key_value_tags={"tag1": None, "tag2": "value2"})

#     expected_tag_filters = [
#         {"Key": "tag1", "Values": []},
#         {"Key": "tag2", "Values": ["value2"]},
#     ]

#     assert query.format_resource_query() == expected_tag_filters


# def test_returns_layer_filter_when_querying_with_layer():
#     query = DatasetFilters(
#         layer="raw",
#     )
#     expected_tag_filters = [
#         {"Key": "layer", "Values": ["raw"]},
#     ]
#     assert query.format_resource_query() == expected_tag_filters


# def test_returns_domain_filter_when_querying_with_domain():
#     query = DatasetFilters(
#         domain="test",
#     )
#     expected_tag_filters = [
#         {"Key": "domain", "Values": ["test"]},
#     ]
#     assert query.format_resource_query() == expected_tag_filters


# def test_returns_tag_filter_list_when_querying_for_sensitivity_level():
#     query = DatasetFilters(sensitivity="PUBLIC")

#     expected_tag_filters = [{"Key": "sensitivity", "Values": ["PUBLIC"]}]

#     assert query.format_resource_query() == expected_tag_filters


# def test_returns_tag_filter_list_when_querying_for_sensitivity_and_tags():
#     query = DatasetFilters(
#         sensitivity=["PUBLIC", "PRIVATE"],
#         key_value_tags={"tag1": None, "tag2": "value2"},
#         key_only_tags=["tag3"],
#         domain="domain",
#         layer="raw",
#     )

#     expected_tag_filters = [
#         {"Key": "tag1", "Values": []},
#         {"Key": "tag2", "Values": ["value2"]},
#         {"Key": "tag3", "Values": []},
#         {"Key": "sensitivity", "Values": ["PUBLIC", "PRIVATE"]},
#         {"Key": "layer", "Values": ["raw"]},
#         {"Key": "domain", "Values": ["domain"]},
#     ]

#     assert query.format_resource_query() == expected_tag_filters


# def test_returns_tag_filter_list_when_querying_for_sensitivity_specifically_and_by_tag():
#     query = DatasetFilters(
#         sensitivity="PRIVATE", key_value_tags={"sensitivity": "PRIVATE"}
#     )

#     with pytest.raises(
#         UserError,
#         match="You cannot specify sensitivity both at the root level and in the tags",
#     ):
#         query.format_resource_query()
