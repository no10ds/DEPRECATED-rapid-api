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
    subjects = subject_service.list_subjects()

    users = [
        {"subject_id": subject["subject_id"], "subject_name": subject["subject_name"]}
        for subject in subjects
        if subject["type"] == "USER"
    ]

    clients = [
        {"subject_id": subject["subject_id"], "subject_name": subject["subject_name"]}
        for subject in subjects
        if subject["type"] == "CLIENT"
    ]

    grouped_subjects = {"clients": clients, "users": users}

    return templates.TemplateResponse(
        name="subject.html", context={"request": request, "subjects": grouped_subjects}
    )


@subject_management_router.get(
    "/{subject_id}/modify/success",
    dependencies=[Security(secure_endpoint, scopes=[Action.USER_ADMIN.value])],
)
def modify_subject_success(request: Request, subject_id: str):
    subject_permission_display_names = []
    subject_name = None
    error_message = ""

    try:
        subject_name = subject_service.get_subject_name_by_id(subject_id)
        subject_permissions = permissions_service.get_user_permissions_ui(subject_id)

        _get_permission_display_name(
            subject_permission_display_names, subject_permissions
        )

    except AWSServiceError:
        error_message = "Something went wrong. Please contact your system administrator"

    return templates.TemplateResponse(
        name="success.html",
        context={
            "request": request,
            "subject_id": subject_id,
            "subject_name": subject_name,
            "subject_permissions_display_names": subject_permission_display_names,
            "error_message": error_message,
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


def _get_permission_display_name(subject_permission_display_names, subject_permissions):
    for inner_list_permission in subject_permissions.values():
        for permission in inner_list_permission:
            subject_permission_display_names.append(permission["display_name_full"])
