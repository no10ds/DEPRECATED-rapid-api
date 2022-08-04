from fastapi import APIRouter, Security
from fastapi import status as http_status

from api.application.services.authorisation.authorisation_service import secure_endpoint
from api.application.services.permissions_service import PermissionsService
from api.common.config.auth import Action

permissions_service = PermissionsService()

permissions_router = APIRouter(
    prefix="/permissions",
    tags=["Permissions"],
    responses={404: {"description": "Not found"}},
)


@permissions_router.get(
    "",
    status_code=http_status.HTTP_200_OK,
    dependencies=[Security(secure_endpoint, scopes=[Action.USER_ADMIN.value])],
)
async def get_permissions():
    return permissions_service.get_permissions()
