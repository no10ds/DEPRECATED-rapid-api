from typing import Optional

from fastapi import APIRouter, Request
from fastapi import UploadFile, File, Response, Security
from fastapi import status as http_status
from pandas import DataFrame
from starlette.responses import PlainTextResponse

from api.adapter.athena_adapter import AthenaAdapter
from api.adapter.aws_resource_adapter import AWSResourceAdapter
from api.application.services.authorisation.authorisation_service import (
    secure_dataset_endpoint,
    secure_endpoint,
)
from api.application.services.data_service import DataService
from api.application.services.delete_service import DeleteService
from api.application.services.format_service import FormatService
from api.common.config.auth import Action
from api.common.custom_exceptions import (
    CrawlerStartFailsError,
    SchemaNotFoundError,
    UserError,
)
from api.common.logger import AppLogger
from api.domain.dataset_filters import DatasetFilters
from api.domain.mime_type import MimeType
from api.domain.sql_query import SQLQuery

resource_adapter = AWSResourceAdapter()
data_service = DataService()
athena_adapter = AthenaAdapter()
delete_service = DeleteService()

datasets_router = APIRouter(
    prefix="/datasets",
    tags=["Datasets"],
    responses={404: {"description": "Not found"}},
)


@datasets_router.post(
    "",
    dependencies=[Security(secure_endpoint, scopes=[Action.READ.value])],
    status_code=http_status.HTTP_200_OK,
)
async def list_all_datasets(tag_filters: DatasetFilters = DatasetFilters()):
    """
    ## List datasets

    Use this endpoint to retrieve a list of available datasets. You can also filter by the dataset sensitivity level or by
    tags specified on the dataset.

    If you do not specify any filter values, you will retrieve all available datasets.

    ### Inputs

    | Parameters    | Usage                                   | Example values                                                                                         | Definition            |
    |---------------|-----------------------------------------|------------------------------------------------------------------------------------------------------- |-----------------------|
    | query         | JSON Request Body                       | Consult the [docs](https://github.com/no10ds/rapid-api/blob/main/docs/guides/usage/usage.md#examples-2)| the filtering query   |

    ### Accepted permissions

    You will always be able to list all available datasets, regardless of their sensitivity level, provided you have
    a `READ` scope, e.g.: `READ_ALL`, `READ_PUBLIC`, `READ_PRIVATE`, `READ_PROTECTED_{DOMAIN}`

    ### Click  `Try it out` to use the endpoint

    """
    return resource_adapter.get_datasets_metadata(tag_filters)


@datasets_router.get(
    "/{domain}/{dataset}/info",
    dependencies=[Security(secure_dataset_endpoint, scopes=[Action.READ.value])],
)
async def get_dataset_info(domain: str, dataset: str):
    """
    ## Dataset info

    Use this endpoint to retrieve basic information for specific datasets, if there is no data stored for the dataset and
    error will be thrown.

    When a valid dataset is retrieved the available data will be the schema definition with some extra values such as:

    - number of rows
    - number of columns
    - statistics data for date columns

    ### Inputs

    | Parameters    | Usage                                   | Example values               | Definition            |
    |---------------|-----------------------------------------|------------------------------|-----------------------|
    | `domain`      | URL parameter                           | `land`                       | domain of the dataset |
    | `dataset`     | URL parameter                           | `train_journeys`             | dataset title         |

    ### Accepted permissions

    You will always be able to get info on all available datasets, regardless of their sensitivity level, provided you have
    a `READ` scope, e.g.: `READ_ALL`, `READ_PUBLIC`, `READ_PRIVATE`, `READ_PROTECTED_{DOMAIN}`

    ### Click  `Try it out` to use the endpoint

    """
    return data_service.get_dataset_info(domain, dataset)


@datasets_router.get(
    "/{domain}/{dataset}/files",
    dependencies=[Security(secure_dataset_endpoint, scopes=[Action.READ.value])],
)
async def list_raw_files(domain: str, dataset: str):
    """
    ## List Raw Files

    Use this endpoint to retrieve all raw files linked to a specific domain/dataset, if there is no data stored for the
    domain/dataset an error will be thrown.

    When a valid domain/dataset is retrieved the available raw file uploads will be displayed in list format.

    ### Inputs

    | Parameters    | Usage                                   | Example values               | Definition            |
    |---------------|-----------------------------------------|------------------------------|-----------------------|
    | `domain`      | URL parameter                           | `land`                       | domain of the dataset |
    | `dataset`     | URL parameter                           | `train_journeys`             | dataset title         |

    ### Outputs

    List of raw files in json format in the response body:

    ```json
    [
    "2022-01-21T17:12:31-file1.csv",
    "2022-01-24T11:43:28-file2.csv"
    ]
    ```

    ### Accepted permissions

    You will always be able to get info on all available datasets, regardless of their sensitivity level, provided you have
    a `READ` scope, e.g.: `READ_ALL`, `READ_PUBLIC`, `READ_PRIVATE`, `READ_PROTECTED_{DOMAIN}`

    ### Click  `Try it out` to use the endpoint

    """
    raw_files = data_service.list_raw_files(domain, dataset)
    return raw_files


@datasets_router.delete(
    "/{domain}/{dataset}/{filename}",
    dependencies=[Security(secure_dataset_endpoint, scopes=[Action.WRITE.value])],
)
async def delete_data_file(
    domain: str, dataset: str, filename: str, response: Response
):
    """
    ## Delete Data File

    Use this endpoint to delete a specific file linked to a domain/dataset. If there is no data stored for the
    domain/dataset or the file name is invalid an error will be thrown.

    When a valid file in the domain/dataset is deleted, a success message will be displayed.

    ### General structure

    `GET /datasets/{domain}/{dataset}/{filename}`

    ### Inputs

    | Parameters | Usage                                   | Example values                  | Definition                    |
    |------------|-----------------------------------------|---------------------------------|-------------------------------|
    | `domain`   | URL parameter                           | `land`                          | domain of the dataset         |
    | `dataset`  | URL parameter                           | `train_journeys`                | dataset title                 |
    | `filename` | URL parameter                           | `2022-01-21T17:12:31-file1.csv` | previously uploaded file name |


    ### Accepted permissions
    In order to use this endpoint you need a relevant WRITE scope that matches the dataset sensitivity level,
    e.g.: `WRITE_ALL`, `WRITE_PUBLIC`, `WRITE_PUBLIC`, `WRITE_PROTECTED_{DOMAIN}`

    ### Click  `Try it out` to use the endpoint

    """
    try:
        delete_service.delete_dataset_file(domain, dataset, filename)
        return Response(status_code=http_status.HTTP_204_NO_CONTENT)
    except CrawlerStartFailsError as error:
        AppLogger.warning("Failed to start crawler: %s", error.args[0])
        response.status_code = http_status.HTTP_202_ACCEPTED
        return {"details": f"{filename} has been deleted."}


@datasets_router.post(
    "/{domain}/{dataset}",
    status_code=http_status.HTTP_201_CREATED,
    dependencies=[Security(secure_dataset_endpoint, scopes=[Action.WRITE.value])],
)
async def upload_data(
    domain: str, dataset: str, response: Response, file: UploadFile = File(...)
):
    """
    ## Upload dataset

    Given a schema has been uploaded you can upload data which matches that schema. Uploading a CSV file via this endpoint
    ensures that the data matches the schema and that it is consistent and sanitised. Should any errors be detected during
    upload, these are sent back in the response to facilitate you fixing the issues.

    ### Inputs

    | Parameters    | Usage                                   | Example values               | Definition              |
    |---------------|-----------------------------------------|------------------------------|-------------------------|
    | `domain`      | URL parameter                           | `air`                        | domain of the dataset   |
    | `dataset`     | URL parameter                           | `passengers_by_airport`      | dataset title           |
    | `file`        | File in form data with key value `file` | `passengers_by_airport.csv`  | the dataset file itself |

    #### Domain and dataset

    The domain and dataset names must adhere to the following conditions:

    - Alphanumeric
    - Cannot contain `-` and `/` symbol

    ### Output

    If successful returns file name with a timestamp included, e.g.:

    ```json
    {
    "uploaded": "2022-01-01T13:00:00-passengers_by_airport.csv"
    }
    ```

    ### Accepted permissions

    In order to use this endpoint you need a relevant `WRITE` scope that matches the dataset sensitivity level,
    e.g.: `WRITE_ALL`, `WRITE_PUBLIC`, `WRITE_PRIVATE`, `WRITE_PROTECTED_{DOMAIN}`

    ### Click  `Try it out` to use the endpoint

    """
    try:
        file_contents = await file.read()
        filename = data_service.upload_dataset(
            domain, dataset, file.filename, file_contents
        )
        return {"details": filename.replace(".parquet", "")}
    except SchemaNotFoundError as error:
        AppLogger.warning("Schema not found: %s", error.args[0])
        raise UserError(message=error.args[0])
    except CrawlerStartFailsError as error:
        AppLogger.warning("Failed to start crawler: %s", error.args[0])
        response.status_code = http_status.HTTP_202_ACCEPTED
        return {"details": file.filename}


@datasets_router.post(
    "/{domain}/{dataset}/query",
    dependencies=[Security(secure_dataset_endpoint, scopes=[Action.READ.value])],
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
    """
    ## Query dataset

    Data can be queried provided data has been uploaded at some point in the past and the 'crawler' has completed its run.

    ### Inputs

    | Parameters    | Required     | Usage                   | Example values                                                                                                              | Definition                    |
    |---------------|--------------|-------------------------|-----------------------------------------------------------------------------------------------------------------------------|-------------------------------|
    | `domain`      | True         | URL parameter           | `space`                                                                                                                     | domain of the dataset         |
    | `dataset`     | True         | URL parameter           | `rocket_launches`                                                                                                           | dataset title                 |
    | `query`       | False        | JSON Request Body       | Consult the [docs](https://github.com/no10ds/rapid-api/blob/main/docs/guides/usage/usage.md#how-to-construct-a-query-object)| the query object              |


    ### Outputs

    #### JSON

    By default, the result of the query are returned in JSON format where each key represents a row, e.g.:

    ```json
    {
        "0": {
            "column1": "value1",
            "column2": "value2"
        },
        ...
    }
    ```

    #### CSV

    To get a CSV response, the `Accept` Header has to be set to `text/csv`, this can be set below. The response will come as a table, e.g.:

    ```csv
    "","column1","column2"
    0,"value1","value2"
    ...
    ```

    ### Accepted permissions

    In order to use this endpoint you need a `READ` scope with appropriate sensitivity level permission,
    e.g.: `READ_ALL`, `READ_PUBLIC`, `READ_PRIVATE`, `READ_PROTECTED_{DOMAIN}`

    ### Click  `Try it out` to use the endpoint

    """
    df = athena_adapter.query(domain, dataset, query)
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
