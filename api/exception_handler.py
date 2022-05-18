import json
from typing import List

from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.responses import RedirectResponse

from api.application.services.authorisation_service import (
    UserCredentialsUnavailableError,
)
from api.common.custom_exceptions import SchemaError, BaseAppException
from api.common.logger import AppLogger


def add_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(Exception)
    async def general_handler(request, exc):
        AppLogger.error(f"Uncaught exception: {exc}")
        return JSONResponse(
            content={"details": "Something went wrong. Contact your administrator."},
            status_code=500,
        )

    @app.exception_handler(UserCredentialsUnavailableError)
    async def user_credentials_missing_handler(request, exc):
        return RedirectResponse(url="/login")

    @app.exception_handler(BaseAppException)
    async def base_app_handler(request, exc):
        return JSONResponse(
            content={"details": exc.message}, status_code=exc.status_code
        )

    @app.exception_handler(SchemaError)
    async def schema_error_handler(request, exc):
        AppLogger.warning(f"Invalid schema generated: {exc.message}")
        return JSONResponse(
            content={"details": exc.message}, status_code=exc.status_code
        )

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
