from typing import Optional

from fastapi import APIRouter, Request
from fastapi import UploadFile, File, HTTPException, Response, Security
from fastapi import status as http_status
from pandas import DataFrame
from starlette.responses import PlainTextResponse

from api.adapter.athena_adapter import DatasetQuery
from api.adapter.aws_resource_adapter import AWSResourceAdapter
from api.application.services.authorisation_service import (
    protect_dataset_endpoint,
    protect_endpoint,
)
from api.application.services.data_service import DataService
from api.application.services.delete_service import DeleteService
from api.application.services.format_service import FormatService
from api.common.config.auth import Action
from api.common.custom_exceptions import (
    CrawlerStartFailsError,
    SchemaNotFoundError,
    CrawlerIsNotReadyError,
    GetCrawlerError,
    AWSServiceError,
    UserError,
)
from api.common.logger import AppLogger
from api.controller.utils import _response_body
from api.domain.dataset_filter_query import DatasetFilterQuery
from api.domain.mime_type import MimeType
from api.domain.sql_query import SQLQuery

resource_adapter = AWSResourceAdapter()
data_service = DataService()
query_adapter = DatasetQuery()
delete_service = DeleteService()

datasets_router = APIRouter(
    prefix="/datasets",
    tags=["Datasets"],
    responses={404: {"description": "Not found"}},
)


@datasets_router.post(
    "",
    dependencies=[Security(protect_endpoint, scopes=[Action.READ.value])],
    status_code=http_status.HTTP_200_OK,
)
async def list_all_datasets(tag_filters: DatasetFilterQuery = DatasetFilterQuery()):
    return resource_adapter.get_datasets_metadata(tag_filters)


@datasets_router.get(
    "/{domain}/{dataset}/info",
    dependencies=[Security(protect_dataset_endpoint, scopes=[Action.READ.value])],
)
async def get_dataset_info(domain: str, dataset: str):
    try:
        dataset_info = data_service.get_dataset_info(domain, dataset)
        return dataset_info
    except SchemaNotFoundError as error:
        AppLogger.warning("Schema not found: %s", error.args[0])
        raise HTTPException(status_code=400, detail=error.args[0])


@datasets_router.get(
    "/{domain}/{dataset}/files",
    dependencies=[Security(protect_dataset_endpoint, scopes=[Action.READ.value])],
)
async def list_raw_files(domain: str, dataset: str):
    raw_files = data_service.list_raw_files(domain, dataset)
    return raw_files


@datasets_router.delete(
    "/{domain}/{dataset}/{filename}",
    dependencies=[Security(protect_dataset_endpoint, scopes=[Action.DELETE.value])],
)
async def delete_data_file(
    domain: str, dataset: str, filename: str, response: Response
):
    try:
        delete_service.delete_dataset_file(domain, dataset, filename)
        return Response(status_code=http_status.HTTP_204_NO_CONTENT)
    except CrawlerIsNotReadyError as error:
        AppLogger.warning("File deletion did not occur: %s", error.args[0])
        raise UserError(
            message="Unable to delete file. Please try again later.", status_code=429
        )
    except CrawlerStartFailsError as error:
        AppLogger.warning("Failed to start crawler: %s", error.args[0])
        response.status_code = http_status.HTTP_202_ACCEPTED
        return {f"{filename} has been deleted."}


@datasets_router.post(
    "/{domain}/{dataset}",
    status_code=http_status.HTTP_201_CREATED,
    dependencies=[Security(protect_dataset_endpoint, scopes=[Action.WRITE.value])],
)
async def upload_data(
    domain: str, dataset: str, response: Response, file: UploadFile = File(...)
):
    try:
        file_contents = await file.read()
        filename = data_service.upload_dataset(
            domain, dataset, file.filename, file_contents
        )
        return _response_body(filename)
    except SchemaNotFoundError as error:
        AppLogger.warning("Schema not found: %s", error.args[0])
        raise UserError(message=error.args[0])
    except CrawlerIsNotReadyError as error:
        AppLogger.warning("Data was not uploaded: %s", error.args[0])
        raise UserError(
            message="Data is currently processing. Please try again later.",
            status_code=429,
        )
    except GetCrawlerError as error:
        AppLogger.error(error.args[0])
        raise AWSServiceError(message="Internal failure when uploading data.")
    except CrawlerStartFailsError as error:
        AppLogger.warning("Failed to start crawler: %s", error.args[0])
        response.status_code = http_status.HTTP_202_ACCEPTED
        return _response_body(file.filename)


@datasets_router.post(
    "/{domain}/{dataset}/query",
    dependencies=[Security(protect_dataset_endpoint, scopes=[Action.READ.value])],
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        0: {"col1": "123", "col2": "something", "col3": "500"},
                        1: {"col1": "456", "col2": "something else", "col3": "600"},
                    }
                },
                "text/csv": {
                    "example": 'col1;col2;col3\n"123","something","500"\n"456","something else","600"'
                },
            }
        }
    },
)
async def query_dataset(
    domain: str, dataset: str, request: Request, query: Optional[SQLQuery] = SQLQuery()
):
    df = query_adapter.query(domain, dataset, query)
    string_df = df.astype("string")
    output_format = request.headers.get("Accept")
    mime_type = MimeType.to_mimetype(output_format)
    return _format_query_output(string_df, mime_type)


def _format_query_output(df: DataFrame, mime_type: MimeType) -> Response:
    formatted_output = FormatService.from_df_to_mimetype(df, mime_type)
    if mime_type == MimeType.TEXT_CSV:
        return PlainTextResponse(status_code=200, content=formatted_output)
    else:
        return formatted_output
