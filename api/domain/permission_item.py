from typing import Optional

from pydantic import BaseModel
from api.common.config.auth import LayerPermissions


class PermissionItem(BaseModel):
    id: str
    type: str
    sensitivity: Optional[str] = None
    domain: Optional[str] = None
    layer: Optional[LayerPermissions] = None

    def to_dict(self):
        return {
            "PermissionName": self.id,
            "Type": self.type,
            "Sensitivity": self.sensitivity,
            "Domain": self.domain,
            "Layer": self.layer,
        }
