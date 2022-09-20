import os
from typing import List, Dict

from fastapi import APIRouter
from fastapi import Request, Security
from fastapi.templating import Jinja2Templates

from api.application.services.authorisation.authorisation_service import secure_endpoint
from api.application.services.permissions_service import PermissionsService
from api.application.services.subject_service import SubjectService
from api.common.config.auth import Action

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

    users = sorted(
        [
            {
                "subject_id": subject["subject_id"],
                "subject_name": subject["subject_name"],
            }
            for subject in subjects
            if subject["type"] == "USER"
        ],
        key=lambda client: client["subject_name"],
    )

    clients = sorted(
        [
            {
                "subject_id": subject["subject_id"],
                "subject_name": subject["subject_name"],
            }
            for subject in subjects
            if subject["type"] == "CLIENT"
        ],
        key=lambda client: client["subject_name"],
    )

    grouped_subjects = {"Client Apps": clients, "Users": users}

    return templates.TemplateResponse(
        name="subject.html", context={"request": request, "subjects": grouped_subjects}
    )


@subject_management_router.get(
    "/{subject_id}/modify/success",
    dependencies=[Security(secure_endpoint, scopes=[Action.USER_ADMIN.value])],
)
def modify_subject_success(request: Request, subject_id: str):
    subject_name = subject_service.get_subject_name_by_id(subject_id)
    subject_permissions = permissions_service.get_user_permissions_ui(subject_id)
    subject_permission_display_names = _get_permission_display_name(subject_permissions)

    return templates.TemplateResponse(
        name="success.html",
        context={
            "request": request,
            "subject_id": subject_id,
            "subject_name": subject_name,
            "subject_permissions_display_names": subject_permission_display_names,
        },
    )


@subject_management_router.get(
    "/{subject_id}/modify",
    dependencies=[Security(secure_endpoint, scopes=[Action.USER_ADMIN.value])],
)
def modify_subject(request: Request, subject_id: str):
    subject_permissions = permissions_service.get_user_permissions_ui(subject_id)
    all_permissions = permissions_service.get_all_permissions_ui()
    subject_name = subject_service.get_subject_name_by_id(subject_id)

    return templates.TemplateResponse(
        name="subject_modify.html",
        context={
            "request": request,
            "subject_id": subject_id,
            "subject_name": subject_name,
            "permissions": all_permissions,
            "subject_permissions": subject_permissions,
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
        headers={"Cache-Control": "no-store"},
    )


def _get_permission_display_name(
    subject_permissions: Dict[str, List[Dict[str, str]]]
) -> List[str]:
    subject_permission_display_names = []
    for inner_list_permission in subject_permissions.values():
        for permission in inner_list_permission:
            subject_permission_display_names.append(permission["display_name_full"])
    return subject_permission_display_names
