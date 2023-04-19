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
            "id": self.id,
            "type": self.type,
            "layer": self.layer,
            "sensitivity": self.sensitivity,
            "domain": self.domain,
        }
