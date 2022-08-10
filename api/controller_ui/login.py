import os

from fastapi import APIRouter
from fastapi.templating import Jinja2Templates
from starlette.requests import Request
from starlette.responses import RedirectResponse
from starlette.status import HTTP_302_FOUND

from api.application.services.authorisation.authorisation_service import user_logged_in
from api.common.aws_utilities import get_secret
from api.common.config.auth import (
    COGNITO_USER_LOGIN_APP_CREDENTIALS_SECRETS_NAME,
    construct_user_auth_url,
    construct_logout_url,
    RAPID_ACCESS_TOKEN,
)

login_router = APIRouter(
    prefix="", responses={404: {"description": "Not found"}}, include_in_schema=False
)

templates = Jinja2Templates(directory=(os.path.abspath("templates")))


@login_router.get("/login")
def login(request: Request):
    if user_logged_in(request):
        return RedirectResponse(url="/", status_code=HTTP_302_FOUND)
    cognito_user_login_client_id = get_secret(
        COGNITO_USER_LOGIN_APP_CREDENTIALS_SECRETS_NAME
    )["client_id"]
    user_auth_url = construct_user_auth_url(cognito_user_login_client_id)
    return templates.TemplateResponse(
        name="login.html", context={"request": request, "auth_url": user_auth_url}
    )


@login_router.get("/logout")
def logout():
    cognito_user_login_client_id = get_secret(
        COGNITO_USER_LOGIN_APP_CREDENTIALS_SECRETS_NAME
    )["client_id"]
    logout_url = construct_logout_url(cognito_user_login_client_id)
    redirect_response = RedirectResponse(url=logout_url, status_code=HTTP_302_FOUND)
    redirect_response.delete_cookie(RAPID_ACCESS_TOKEN)
    return redirect_response
