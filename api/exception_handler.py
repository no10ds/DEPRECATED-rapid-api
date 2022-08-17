import json
import os
from typing import List

from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.templating import Jinja2Templates
from starlette.responses import RedirectResponse

from api.application.services.authorisation.authorisation_service import (
    UserCredentialsUnavailableError,
    is_browser_request,
)
from api.common.custom_exceptions import (
    BaseAppException,
    NotAuthorisedToViewPageError,
    SchemaError,
    SchemaNotFoundError,
)
from api.common.logger import AppLogger

templates = Jinja2Templates(directory=(os.path.abspath("templates")))


def add_exception_handlers(app: FastAPI) -> None:
    # Custom handlers
    @app.exception_handler(UserCredentialsUnavailableError)
    async def user_credentials_missing_handler(request, exc):
        return RedirectResponse(url="/login")

    @app.exception_handler(NotAuthorisedToViewPageError)
    async def not_authorised_to_view_page_handler(request, exc):
        return RedirectResponse(url="/")

    @app.exception_handler(SchemaError)
    async def schema_error_handler(request, exc):
        AppLogger.warning(f"Invalid schema generated: {exc.message}")
        return JSONResponse(
            content={"details": exc.message}, status_code=exc.status_code
        )

    @app.exception_handler(SchemaNotFoundError)
    async def schema_not_found_handler(request, exc):
        message = exc.message if exc.message else "Schema not found."
        AppLogger.warning("Schema not found: %s", message)
        if is_browser_request(request):
            return templates.TemplateResponse(
                name="error.html",
                context={"request": request, "error_message": exc.message},
            )
        else:
            return JSONResponse(content={"details": message}, status_code=400)

    @app.exception_handler(BaseAppException)
    async def base_app_handler(request, exc):
        if is_browser_request(request):
            return templates.TemplateResponse(
                name="error.html",
                context={"request": request, "error_message": exc.message},
            )
        else:
            return JSONResponse(
                content={"details": exc.message}, status_code=exc.status_code
            )

    @app.exception_handler(Exception)
    async def general_handler(request, exc):
        try:
            message = exc.message
            status_code = exc.status_code
        except AttributeError:
            message = "Something went wrong. Please contact your system administrator."
            status_code = 500

        if is_browser_request(request):
            return templates.TemplateResponse(
                name="error.html",
                context={"request": request, "error_message": message},
            )
        else:
            return JSONResponse(content={"details": message}, status_code=status_code)

    # Override handlers
    @app.exception_handler(RequestValidationError)
    async def pydantic_error_handler(request, exc):
        return JSONResponse(
            content={"details": _generate_pydantic_error_message(exc.json())},
            status_code=400,
        )

    def _generate_pydantic_error_message(json_message: json) -> List[str]:
        PYDANTIC_JSON_DECODE_ERROR = "value_error.jsondecode"

        error_messages = []

        for error in json.loads(json_message):
            if error.get("type") == PYDANTIC_JSON_DECODE_ERROR:
                error_output = error.get("msg")
            else:
                error_output = _format_error_message_with_location(error)
            error_messages.append(error_output)
        return error_messages

    def _format_error_message_with_location(error):
        location_path = ": ".join([str(item) for item in error.get("loc")[1:]])
        return f"{location_path} -> {error.get('msg')}"
