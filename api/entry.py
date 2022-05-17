import os

import sass
from fastapi import FastAPI, Request, Depends
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.responses import RedirectResponse
from starlette.status import HTTP_302_FOUND

from api.adapter.aws_resource_adapter import AWSResourceAdapter
from api.application.services.authorisation_service import (
    protect_dataset_endpoint,
    user_logged_in,
    RAPID_ACCESS_TOKEN,
    extract_user_groups,
)
from api.common.aws_utilities import get_secret
from api.common.config.auth import (
    COGNITO_USER_LOGIN_APP_CREDENTIALS_SECRETS_NAME,
    construct_user_auth_url,
)
from api.common.config.docs import custom_openapi_docs_generator, COMMIT_SHA, VERSION
from api.common.logger import AppLogger, init_logger
from api.controller.auth import auth_router
from api.controller.client import client_router
from api.controller.datasets import datasets_router
from api.controller.schema import schema_router
from api.controller.protected_domain import protected_domain_router
from api.exception_handler import add_exception_handlers

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
app.openapi = custom_openapi_docs_generator(app)
sass.compile(dirname=("static/sass/main", "static"), output_style="compressed")
resource_adapter = AWSResourceAdapter()
add_exception_handlers(app)

templates = Jinja2Templates(directory=(os.path.abspath("templates")))

app.include_router(auth_router)
app.include_router(datasets_router)
app.include_router(schema_router)
app.include_router(client_router)
app.include_router(protected_domain_router)


@app.on_event("startup")
async def startup_event():
    init_logger()


@app.middleware("http")
async def request_middleware(request: Request, call_next):
    request_id = request.headers.get("x-request-id")
    query_params = request.url.include_query_params()
    AppLogger.info(
        f"    Request started: {request.method} {query_params} with request id: {request_id}"
    )
    return await call_next(request)


@app.get("/status", tags=["Status"])
def status():
    """The endpoint used for service health check"""
    return {"status": "deployed", "sha": COMMIT_SHA, "version": VERSION}


@app.get("/login", include_in_schema=False)
def login(request: Request):
    if user_logged_in(request):
        return RedirectResponse(url="/upload", status_code=HTTP_302_FOUND)
    cognito_user_login_client_id = get_secret(
        COGNITO_USER_LOGIN_APP_CREDENTIALS_SECRETS_NAME
    )["client_id"]
    user_auth_url = construct_user_auth_url(cognito_user_login_client_id)
    return templates.TemplateResponse(
        name="login.html", context={"request": request, "auth_url": user_auth_url}
    )


@app.get(
    "/upload", include_in_schema=False, dependencies=[Depends(protect_dataset_endpoint)]
)
def upload(request: Request):
    datasets = _get_authorised_datasets(request)
    return templates.TemplateResponse(
        name="upload.html", context={"request": request, "datasets": datasets}
    )


@app.get("/logout", include_in_schema=False)
def logout():
    redirect_response = RedirectResponse(url="/login", status_code=HTTP_302_FOUND)
    redirect_response.delete_cookie(RAPID_ACCESS_TOKEN)
    return redirect_response


def _get_authorised_datasets(request):
    scope_prefix = "WRITE/"
    user_groups = extract_user_groups(request.cookies.get(RAPID_ACCESS_TOKEN))
    datasets = _build_list_of_datasets_from_write_permissions(scope_prefix, user_groups)
    datasets.sort()
    return datasets


def _build_list_of_datasets_from_write_permissions(scope_prefix, user_groups):
    return [
        user_group.replace(scope_prefix, "")
        for user_group in user_groups
        if scope_prefix in user_group
    ]
