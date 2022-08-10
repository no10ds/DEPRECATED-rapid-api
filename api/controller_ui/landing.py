import os
from typing import List, Dict

from fastapi import APIRouter
from fastapi import Request, Depends
from fastapi.templating import Jinja2Templates

from api.application.services.authorisation.authorisation_service import (
    RAPID_ACCESS_TOKEN,
    secure_endpoint,
)
from api.application.services.authorisation.token_utils import parse_token
from api.application.services.permissions_service import PermissionsService
from api.common.config.auth import Action

landing_router = APIRouter(
    prefix="", responses={404: {"description": "Not found"}}, include_in_schema=False
)

templates = Jinja2Templates(directory=(os.path.abspath("templates")))

permissions_service = PermissionsService()


@landing_router.get("/", dependencies=[Depends(secure_endpoint)])
def landing(request: Request):
    subject_id = parse_token(request.cookies.get(RAPID_ACCESS_TOKEN)).subject
    subject_permissions = permissions_service.get_subject_permissions(subject_id)
    allowed_actions = determine_user_ui_actions(subject_permissions)
    return templates.TemplateResponse(
        name="index.html", context={"request": request, **allowed_actions}
    )


def determine_user_ui_actions(subject_permissions: List[str]) -> Dict[str, bool]:
    return {
        "can_manage_users": Action.USER_ADMIN.value in subject_permissions,
        "can_upload": any(
            (
                permission.startswith(Action.WRITE.value)
                for permission in subject_permissions
            )
        ),
        "can_download": any(
            (
                permission.startswith(Action.READ.value)
                for permission in subject_permissions
            )
        ),
    }
