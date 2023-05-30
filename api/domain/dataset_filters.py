from typing import Optional, Dict, List, Union

from pydantic.main import BaseModel

from api.common.custom_exceptions import UserError
from api.common.config.auth import Layer


class DatasetFilters(BaseModel):
    layer: Optional[Union[List[Layer], Layer]] = None
    domain: Optional[Union[List[str], str]] = None
    sensitivity: Optional[Union[List[str], str]] = None
    key_value_tags: Optional[Dict[str, Optional[str]]] = dict()
    key_only_tags: Optional[List[str]] = list()

    def format_resource_query(self):
        if self.sensitivity and any(
            [key == "sensitivity" for key, _ in self.key_value_tags.items()]
        ):
            raise UserError(
                "You cannot specify sensitivity both at the root level and in the tags"
            )
        return [
            *self._tag_filters(),
            *self._build_generic_filter("sensitivity", self.sensitivity),
            *self._build_generic_filter("layer", self.layer),
            *self._build_generic_filter("domain", self.domain),
        ]

    def _tag_filters(self) -> List[dict]:
        key_value_tags_dict_list = self._build_key_value_tags()

        key_only_tags_dict_list = self._build_key_only_tags()

        return key_value_tags_dict_list + key_only_tags_dict_list

    def _build_key_only_tags(self):
        return [{"Key": key, "Values": []} for key in self.key_only_tags]

    def _build_key_value_tags(self):
        return [
            {"Key": key, "Values": [value] if value is not None and value != "" else []}
            for key, value in self.key_value_tags.items()
        ]

    def _build_generic_filter(
        self, name: str, value: Union[List[str], str]
    ) -> List[Dict]:
        if isinstance(value, list):
            values = value
        else:
            values = [value]

        return [{"Key": name, "Values": values}] if value is not None else []
