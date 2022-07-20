from typing import Optional

from pydantic import BaseModel


class PermissionItem(BaseModel):
    id: str
    sensitivity: Optional[str]
    type: str
