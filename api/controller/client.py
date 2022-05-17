from fastapi import APIRouter
from fastapi import Security
from fastapi import status as http_status

from api.application.services.authorisation_service import protect_endpoint
from api.application.services.client_service import ClientService
from api.common.config.auth import Action
from api.domain.client import ClientRequest

client_service = ClientService()

client_router = APIRouter(
    prefix="/client",
    tags=["Client"],
    responses={404: {"description": "Not found"}},
)


@client_router.post(
    "",
    status_code=http_status.HTTP_201_CREATED,
    dependencies=[Security(protect_endpoint, scopes=[Action.USER_ADMIN.value])],
)
async def create_client(client_request: ClientRequest):
    client_response = client_service.create_client(client_request)
    return client_response
