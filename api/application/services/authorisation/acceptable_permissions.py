from dataclasses import dataclass
from typing import Set, List

from api.common.config.auth import Action, SensitivityLevel, ALL
from api.common.config.layers import Layer


@dataclass
class AcceptablePermissions:
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


def generate_acceptable_scopes(
    endpoint_actions: List[str],
    sensitivity: SensitivityLevel,
    layer: Layer = None,
    domain: str = None,
) -> AcceptablePermissions:
    endpoint_actions = [Action.from_string(action) for action in endpoint_actions]

    required_scopes = set()
    optional_scopes = set()

    for action in endpoint_actions:

        if action in Action.standalone_actions():
            required_scopes.add(action.value)
            continue

        acceptable_sensitivities = _get_acceptable_sensitivity_values(
            domain, sensitivity
        )

        acceptable_layer_scopes = [layer.upper(), ALL]

        optional_scopes.add(f"{action.value}_{ALL}")

        for acceptable_sensitivity in acceptable_sensitivities:
            for acceptable_layer in acceptable_layer_scopes:
                optional_scopes.add(
                    f"{action.value}_{acceptable_layer}_{acceptable_sensitivity}"
                )

    return AcceptablePermissions(required_scopes, optional_scopes)


def _get_acceptable_sensitivity_values(
    domain: str, sensitivity: SensitivityLevel
) -> List[str]:
    if sensitivity == SensitivityLevel.PROTECTED:
        return [f"{SensitivityLevel.PROTECTED.value}_{domain.upper()}"]
    else:
        implied_sensitivity_map = {
            # The levels in the values imply the levels in the key
            SensitivityLevel.PUBLIC: [
                SensitivityLevel.PRIVATE,
                SensitivityLevel.PUBLIC,
            ],
            SensitivityLevel.PRIVATE: [
                SensitivityLevel.PRIVATE,
            ],
        }
        acceptable_sensitivities = (
            implied_sensitivity_map.get(sensitivity, [sensitivity])
            if sensitivity
            else []
        )
        return [sensitivity.value for sensitivity in acceptable_sensitivities]
