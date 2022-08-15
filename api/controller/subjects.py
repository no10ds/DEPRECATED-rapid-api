from fastapi import APIRouter
from fastapi import Security
from fastapi import status as http_status

from api.application.services.authorisation.authorisation_service import secure_endpoint
from api.application.services.subject_service import SubjectService
from api.common.config.auth import Action

subject_service = SubjectService()

subjects_router = APIRouter(
    prefix="/subjects",
    tags=["Subjects"],
    responses={404: {"description": "Not found"}},
)


@subjects_router.get(
    "",
    status_code=http_status.HTTP_200_OK,
    dependencies=[Security(secure_endpoint, scopes=[Action.USER_ADMIN.value])],
)
async def list_subjects():
    """
    This endpoint lists all user and client apps, returning their username or client app name and their corresponding ID

    ### Click  `Try it out` to use the endpoint

    """
    return subject_service.list_subjects()
