import os

from fastapi import APIRouter, Security
from fastapi import Request, Depends
from fastapi.templating import Jinja2Templates

from api.application.services.authorisation.authorisation_service import (
    RAPID_ACCESS_TOKEN,
    secure_dataset_endpoint,
    secure_endpoint,
)
from api.application.services.authorisation.token_utils import parse_token
from api.application.services.dataset_service import DatasetService
from api.common.config.auth import Action

data_management_router = APIRouter(
    prefix="", responses={404: {"description": "Not found"}}, include_in_schema=False
)

templates = Jinja2Templates(directory=(os.path.abspath("templates")))

upload_service = DatasetService()


@data_management_router.get(
    "/download", dependencies=[Security(secure_endpoint, scopes=[Action.READ.value])]
)
def select_dataset(request: Request):
    grouped_datasets = None

    return templates.TemplateResponse(
        name="datasets.html", context={"request": request, "datasets": grouped_datasets}
    )


@data_management_router.get("/upload", dependencies=[Depends(secure_dataset_endpoint)])
def upload(request: Request):
    subject_id = parse_token(request.cookies.get(RAPID_ACCESS_TOKEN)).subject
    datasets = upload_service.get_authorised_datasets(subject_id)
    return templates.TemplateResponse(
        name="upload.html", context={"request": request, "datasets": datasets}
    )
