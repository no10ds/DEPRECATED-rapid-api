from typing import Optional

from api.common.config.auth import Action


class PermissionItem:
    id: str
    sensitivity: Optional[str]
    type: str
    permission: str

    def __init__(
        self,
        perm_id: str,
        sensitivity: Optional[str],
        perm_type: str,
    ):
        self.id = perm_id
        self.sensitivity = sensitivity
        self.type = perm_type
        self.permission = self._generate_permission()

    def _generate_permission(self) -> str:
        if self.type in Action.standalone_action_values():
            return self.type

        return f"{self.type}_{self.sensitivity}"
