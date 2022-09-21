import sass
from fastapi import FastAPI, Request
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from api.application.services.authorisation.authorisation_service import (
    get_client_token,
    get_user_token,
)
from api.application.services.authorisation.token_utils import parse_token
from api.common.config.docs import custom_openapi_docs_generator, COMMIT_SHA, VERSION
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
from api.controller_ui.data_management import data_management_router
from api.controller_ui.task_management import jobs_ui_router
from api.controller_ui.landing import landing_router
from api.controller_ui.login import login_router
from api.controller_ui.subject_management import subject_management_router
from api.exception_handler import add_exception_handlers

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
app.openapi = custom_openapi_docs_generator(app)
sass.compile(dirname=("static/sass/main", "static"), output_style="compressed")
add_exception_handlers(app)

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
def status():
    """The endpoint used for service health check"""
    return {"status": "deployed", "sha": COMMIT_SHA, "version": VERSION}


@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    return FileResponse("static/favicon.ico")


def _get_subject_id(request: Request):
    client_token = get_client_token(request)
    user_token = get_user_token(request)
    token = client_token if client_token else user_token
    return parse_token(token).subject if token else "Not an authenticated user"


def _set_security_headers(response) -> None:
    if "Cache-Control" not in response.headers:
        response.headers["Cache-Control"] = "private, max-age=3600"

    response.headers["Content-Security-Policy"] = (
        "default-src 'self'; "
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
