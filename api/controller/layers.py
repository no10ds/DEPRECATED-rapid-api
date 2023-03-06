from fastapi import APIRouter
from fastapi import Security
from fastapi import status as http_status

from api.application.services.authorisation.authorisation_service import secure_endpoint
from api.common.config.constants import BASE_API_PATH
from api.common.config.layers import Layer


layers_router = APIRouter(
    prefix=f"{BASE_API_PATH}/layers",
    tags=["Layers"],
    responses={404: {"description": "Not found"}},
)


@layers_router.get(
    "",
    status_code=http_status.HTTP_200_OK,
    dependencies=[Security(secure_endpoint)],
)
async def list_layers():
    """"""
    print(Layer)
    return {"data": 1}
