import os

from fastapi import APIRouter
from fastapi import Request, Security
from fastapi.templating import Jinja2Templates

from api.application.services.authorisation.authorisation_service import secure_endpoint
from api.application.services.permissions_service import PermissionsService
from api.application.services.subject_service import SubjectService
from api.common.config.auth import Action
from api.common.custom_exceptions import UserError, AWSServiceError

permissions_service = PermissionsService()
subject_service = SubjectService()

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
    "/{subject_id}/modify/success",
    dependencies=[Security(secure_endpoint, scopes=[Action.USER_ADMIN.value])],
)
def modify_subject_success(request: Request, subject_id: str):
    try:
        subject_name = subject_service.get_subject_name_by_id(subject_id)
    except UserError:
        subject_name = None

    return templates.TemplateResponse(
        name="success.html",
        context={
            "request": request,
            "subject_id": subject_id,
            "subject_name": subject_name,
        },
    )


@subject_management_router.get(
    "/{subject_id}/modify",
    dependencies=[Security(secure_endpoint, scopes=[Action.USER_ADMIN.value])],
)
def modify_subject(request: Request, subject_id: str):
    error_message = None
    subject_name = None

    try:
        subject_permissions = permissions_service.get_user_permissions_ui(subject_id)
        all_permissions = permissions_service.get_all_permissions_ui()
        subject_name = subject_service.get_subject_name_by_id(subject_id)
    except UserError as error:
        subject_permissions, all_permissions = [], []
        error_message = f"Error: {error.message}. Please go back and try again."
    except AWSServiceError:
        subject_permissions, all_permissions = [], []
        error_message = "Something went wrong. Please contact your system administrator"

    return templates.TemplateResponse(
        name="subject_modify.html",
        context={
            "request": request,
            "subject_id": subject_id,
            "subject_name": subject_name,
            "permissions": all_permissions,
            "subject_permissions": subject_permissions,
            "error_message": error_message,
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
