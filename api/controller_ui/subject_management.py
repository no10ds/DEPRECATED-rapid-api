import os

from fastapi import APIRouter
from fastapi import Request, Security
from fastapi.templating import Jinja2Templates

from api.application.services.authorisation.authorisation_service import (
    secure_endpoint,
)
from api.common.config.auth import Action

subject_management_router = APIRouter(
    prefix="/subject",
    responses={404: {"description": "Not found"}},
    include_in_schema=False,
)

templates = Jinja2Templates(directory=(os.path.abspath("templates")))


@subject_management_router.get(
    "", dependencies=[Security(secure_endpoint, scopes=[Action.USER_ADMIN.value])]
)
def modify_subject(request: Request):
    return templates.TemplateResponse(name="subject.html", context={"request": request})
