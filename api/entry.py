import sass
import os
from fastapi import FastAPI, Request, HTTPException, Security
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from starlette.status import HTTP_404_NOT_FOUND, HTTP_200_OK

from api.application.services.authorisation.authorisation_service import (
    get_client_token,
    get_user_token,
    secure_endpoint,
    RAPID_ACCESS_TOKEN,
)
from api.application.services.authorisation.token_utils import parse_token
from api.application.services.permissions_service import PermissionsService
from api.application.services.dataset_service import DatasetService
from api.common.config.auth import IDENTITY_PROVIDER_BASE_URL, Action
from api.common.config.docs import custom_openapi_docs_generator, COMMIT_SHA, VERSION
from api.common.config.constants import BASE_API_PATH
from api.common.logger import AppLogger, init_logger
from api.controller.auth import auth_router
from api.controller.client import client_router
from api.controller.datasets import datasets_router
from api.controller.jobs import jobs_router
from api.controller.permissions import permissions_router
from api.controller.protected_domain import protected_domain_router
from api.controller.schema import schema_router
from api.controller.subjects import subjects_router
from api.controller.table import table_router
from api.controller.user import user_router
from api.controller_ui.data_management import (
    data_management_router,
    group_datasets_by_domain,
)
from api.controller_ui.task_management import jobs_ui_router
from api.controller_ui.schema_management import schema_management_router
from api.controller_ui.landing import landing_router
from api.controller_ui.login import login_router
from api.controller_ui.subject_management import subject_management_router
from api.exception_handler import add_exception_handlers

PROJECT_NAME = os.environ.get("PROJECT_NAME", None)
PROJECT_DESCRIPTION = os.environ.get("PROJECT_DESCRIPTION", None)
PROJECT_URL = os.environ.get("DOMAIN_NAME", None)
PROJECT_CONTACT = os.environ.get("PROJECT_CONTACT", None)
PROJECT_ORGANISATION = os.environ.get("PROJECT_ORGANISATION", None)

permissions_service = PermissionsService()
upload_service = DatasetService()

app = FastAPI(
    openapi_url=f"{BASE_API_PATH}/openapi.json", docs_url=f"{BASE_API_PATH}/docs"
)
app.mount("/static", StaticFiles(directory="static"), name="static")
app.openapi = custom_openapi_docs_generator(app)
sass.compile(dirname=("static/sass/main", "static"), output_style="compressed")
add_exception_handlers(app)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost", "http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth_router)
app.include_router(permissions_router)
app.include_router(datasets_router)
app.include_router(schema_router)
app.include_router(client_router)
app.include_router(user_router)
app.include_router(protected_domain_router)
app.include_router(login_router)
app.include_router(landing_router)
app.include_router(data_management_router)
app.include_router(subject_management_router)
app.include_router(schema_management_router)
app.include_router(subjects_router)
app.include_router(jobs_router)
app.include_router(jobs_ui_router)
app.include_router(table_router)


@app.on_event("startup")
async def startup_event():
    init_logger()


@app.middleware("http")
async def request_middleware(request: Request, call_next):
    query_params = request.url.include_query_params()
    subject_id = _get_subject_id(request)
    AppLogger.info(
        f"    Request started: {request.method} {query_params} by subject: {subject_id}"
    )
    return await call_next(request)


@app.middleware("http")
async def add_security_headers(request: Request, call_next):
    response = await call_next(request)
    _set_security_headers(response)
    return response


@app.get("/status", tags=["Status"])
def status(request: Request):
    """The endpoint used for service health check"""
    return {
        "status": "deployed",
        "sha": COMMIT_SHA,
        "version": VERSION,
        "root_path": request.scope.get("root_path"),
    }


@app.get(f"{BASE_API_PATH}/apis", tags=["Info"])
def info():
    """The endpoint used for a service information check"""
    if PROJECT_NAME is None:
        raise HTTPException(status_code=HTTP_404_NOT_FOUND, detail="Path not found")

    return {
        "api-version": "api.gov.uk/v1alpha",
        "apis": [
            {
                "api-version": "api.gov.uk/v1alpha",
                "data": {
                    "name": PROJECT_NAME,
                    "description": PROJECT_DESCRIPTION,
                    "url": PROJECT_URL,
                    "contact": PROJECT_CONTACT,
                    "organisation": PROJECT_ORGANISATION,
                    "documentation-url": "https://github.com/no10ds/rapid-api",
                },
            }
        ],
    }


@app.get(
    f"{BASE_API_PATH}/permissions_ui",
    status_code=HTTP_200_OK,
    dependencies=[Security(secure_endpoint, scopes=[Action.USER_ADMIN.value])],
)
async def get_permissions_ui():
    return permissions_service.get_all_permissions_ui()


@app.get(
    f"{BASE_API_PATH}/datasets_ui",
    status_code=HTTP_200_OK,
    dependencies=[Security(secure_endpoint, scopes=[Action.WRITE.value])],
)
async def get_datasets_ui(request: Request):
    subject_id = parse_token(request.cookies.get(RAPID_ACCESS_TOKEN)).subject
    datasets = upload_service.get_authorised_datasets(subject_id, Action.WRITE)

    return group_datasets_by_domain(datasets)


@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    return FileResponse("static/favicon.ico")


def _get_subject_id(request: Request):
    client_token = get_client_token(request)
    user_token = get_user_token(request)
    token = client_token if client_token else user_token
    return parse_token(token).subject if token else "Not an authenticated user"


def _set_security_headers(response) -> None:
    response.headers["Content-Security-Policy"] = (
        "default-src 'self' "
        f"{IDENTITY_PROVIDER_BASE_URL}; "
        "script-src 'self' 'unsafe-inline' "
        "cdn.jsdelivr.net/npm/swagger-ui-dist@3/swagger-ui-bundle.js; "
        "style-src 'self' "
        "cdn.jsdelivr.net/npm/swagger-ui-dist@3/swagger-ui.css; "
        "img-src 'self' data: "
        "fastapi.tiangolo.com/img/favicon.png;"
    )
    response.headers["Content-Security-Policy-Report-Only"] = "default-src 'self'"
    response.headers[
        "Strict-Transport-Security"
    ] = "max-age=31536000 ; includeSubDomains"
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Permitted-Cross-Domain-Policies"] = "none"
    response.headers["Referrer-Policy"] = "strict-origin"
