import os

import sass
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from api.common.config.docs import custom_openapi_docs_generator, COMMIT_SHA, VERSION
from api.common.logger import AppLogger, init_logger
from api.controller.auth import auth_router
from api.controller.client import client_router
from api.controller.datasets import datasets_router
from api.controller.permissions import permissions_router
from api.controller.protected_domain import protected_domain_router
from api.controller.schema import schema_router
from api.controller.user import user_router
from api.controller_ui.data_management import data_management_router
from api.controller_ui.landing import landing_router
from api.controller_ui.login import login_router
from api.controller_ui.subject_management import subject_management_router
from api.exception_handler import add_exception_handlers

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
app.openapi = custom_openapi_docs_generator(app)
sass.compile(dirname=("static/sass/main", "static"), output_style="compressed")
add_exception_handlers(app)

templates = Jinja2Templates(directory=(os.path.abspath("templates")))

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
