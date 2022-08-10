import os

from fastapi import APIRouter
from fastapi import Request, Depends
from fastapi.templating import Jinja2Templates

from api.application.services.UploadService import UploadService
from api.application.services.authorisation.authorisation_service import (
    RAPID_ACCESS_TOKEN,
    secure_dataset_endpoint,
)
from api.application.services.authorisation.token_utils import parse_token

data_management_router = APIRouter(
    prefix="", responses={404: {"description": "Not found"}}, include_in_schema=False
)

templates = Jinja2Templates(directory=(os.path.abspath("templates")))

upload_service = UploadService()


@data_management_router.get("/upload", dependencies=[Depends(secure_dataset_endpoint)])
def upload(request: Request):
    subject_id = parse_token(request.cookies.get(RAPID_ACCESS_TOKEN)).subject
    datasets = upload_service.get_authorised_datasets(subject_id)
    return templates.TemplateResponse(
        name="upload.html", context={"request": request, "datasets": datasets}
    )
