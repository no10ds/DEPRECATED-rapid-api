import os

from fastapi import APIRouter
from fastapi import Request, Security
from fastapi.templating import Jinja2Templates

from api.application.services.authorisation.authorisation_service import secure_endpoint
from api.application.services.permissions_service import PermissionsService
from api.common.config.auth import Action

permissions_service = PermissionsService()

subject_management_router = APIRouter(
    prefix="/subject",
    responses={404: {"description": "Not found"}},
    include_in_schema=False,
)

templates = Jinja2Templates(directory=(os.path.abspath("templates")))


@subject_management_router.get(
    "", dependencies=[Security(secure_endpoint, scopes=[Action.USER_ADMIN.value])]
)
def select_subject(request: Request):
    return templates.TemplateResponse(name="subject.html", context={"request": request})


@subject_management_router.get(
    "/{subject_id}/modify",
    dependencies=[Security(secure_endpoint, scopes=[Action.USER_ADMIN.value])],
)
def modify_subject(request: Request, subject_id: str):
    all_permissions = permissions_service.get_all_permissions_ui()
    user_permissions = permissions_service.get_user_permissions_ui(subject_id)

    return templates.TemplateResponse(
        name="subject_modify.html",
        context={
            "request": request,
            "subject_name": subject_id,
            "permissions": all_permissions,
            "user_permissions": user_permissions,
        },
    )


@subject_management_router.get(
    "/create",
    dependencies=[Security(secure_endpoint, scopes=[Action.USER_ADMIN.value])],
)
def create_subject(request: Request):
    permissions = permissions_service.get_all_permissions_ui()
    return templates.TemplateResponse(
        name="subject_create.html",
        context={"request": request, "permissions": permissions},
    )
