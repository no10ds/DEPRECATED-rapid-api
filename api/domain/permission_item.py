from typing import Optional

from pydantic import BaseModel

from api.common.config.auth import Action


class PermissionItem(BaseModel):
    id: str
    sensitivity: Optional[str]
    type: str

    def generate_permission(self) -> str:
        if self.type in Action.standalone_action_values():
            return self.type

        return f"{self.type}_{self.sensitivity}"
