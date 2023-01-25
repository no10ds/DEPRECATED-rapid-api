import os

from fastapi import APIRouter
from fastapi import Security, Request
from fastapi.templating import Jinja2Templates

from api.application.services.authorisation.authorisation_service import secure_endpoint
from api.common.config.auth import Action
from api.common.config.constants import BASE_API_PATH
from api.application.services.schema_infer_service import SchemaInferService

schema_infer_service = SchemaInferService()

schema_management_router = APIRouter(
    prefix=f"{BASE_API_PATH}/schema",
    responses={404: {"description": "Not found"}},
    include_in_schema=False,
)

templates = Jinja2Templates(directory=os.path.abspath("templates"))


@schema_management_router.get(
    "/create",
    dependencies=[Security(secure_endpoint, scopes=[Action.DATA_ADMIN.value])],
)
def create_schema(request: Request):
    return templates.TemplateResponse(
        name="schema_create.html", context={"request": request}
    )
