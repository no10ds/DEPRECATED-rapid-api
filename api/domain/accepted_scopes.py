from dataclasses import dataclass
from typing import Set, List


@dataclass
class AcceptedScopes:
    required: Set[str]
    optional: Set[str]

    def satisfied_by(self, token_scopes: List[str]) -> bool:
        all_required = all(
            [required_scope in token_scopes for required_scope in self.required]
        )
        any_optional = (
            any([any_scope in token_scopes for any_scope in self.optional])
            if self.optional
            else True
        )

        return all_required and any_optional
