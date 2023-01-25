from fastapi import APIRouter
from fastapi import UploadFile, File, Security
from fastapi import status as http_status

from api.adapter.cognito_adapter import CognitoAdapter
from api.application.services.authorisation.authorisation_service import secure_endpoint
from api.application.services.data_service import DataService
from api.application.services.delete_service import DeleteService
from api.application.services.schema_infer_service import SchemaInferService
from api.common.config.auth import Action
from api.common.config.constants import BASE_API_PATH
from api.common.custom_exceptions import (
    AWSServiceError,
    CrawlerAlreadyExistsError,
    CrawlerCreationError,
)
from api.common.logger import AppLogger
from api.domain.schema import Schema

data_service = DataService()
schema_infer_service = SchemaInferService()
delete_service = DeleteService()
cognito_adapter = CognitoAdapter()

schema_router = APIRouter(
    prefix=f"{BASE_API_PATH}/schema",
    tags=["Schema"],
    responses={404: {"description": "Not found"}},
)


@schema_router.post("/{sensitivity}/{domain}/{dataset}/generate")
async def generate_schema(
    sensitivity: str, domain: str, dataset: str, file: UploadFile = File(...)
):
    """
    ## Generate schema

    In order to upload the dataset for the first time, you need to define its schema. This endpoint is provided for your
    convenience to generate a schema based on an existing dataset. Alternatively you can consult
    the [schema writing guide](https://github.com/no10ds/rapid-api/blob/main/docs/guides/usage/schema_creation.md) if you would like to create the schema yourself. You can then use the
    output of this endpoint in the Schema Upload endpoint.

    ⚠️ WARNING:
    - The first 50MB of the uploaded file (regardless of size) are used to infer the schema
    - Consider uploading a representative sample of your dataset (e.g.: the first 10,000 rows) instead of uploading the entire large file which could take a long time

    ### Inputs

    | Parameters    | Usage                                   | Example values               | Definition                 |
    |---------------|-----------------------------------------|------------------------------|----------------------------|
    | `sensitivity` | URL parameter                           | `PUBLIC, PRIVATE, PROTECTED` | sensitivity of the dataset |
    | `domain`      | URL parameter                           | `demo`                       | domain of the dataset      |
    | `dataset`     | URL parameter                           | `gapminder`                  | dataset title              |
    | `file`        | File in form data with key value `file` | `gapminder.csv`              | the dataset file itself    |

    #### Domain and dataset

    The domain and dataset names must adhere to the following conditions:

    - Only alphanumeric and underscore `_` characters allowed
    - Start with an alphabetic character

    ### Click  `Try it out` to use the endpoint

    """
    infer_contents = get_first_mb_of_file(file)
    return schema_infer_service.infer_schema(
        domain, dataset, sensitivity, infer_contents
    )


def get_first_mb_of_file(file: UploadFile, chunk_size_mb: int = 50) -> bytes:
    mb_1 = 1024 * 1024
    return file.file.read(mb_1 * chunk_size_mb)


@schema_router.post(
    "",
    status_code=http_status.HTTP_201_CREATED,
    dependencies=[Security(secure_endpoint, scopes=[Action.DATA_ADMIN.value])],
)
async def upload_schema(schema: Schema):
    """
    ## Upload Schema

    When you have a schema definition you can use this endpoint to upload it. This will allow you to subsequently upload
    datasets that match the schema. If you do not yet have a schema definition, you can craft this yourself (see
    the [schema writing guide](https://github.com/no10ds/rapid-api/blob/main/docs/guides/usage/schema_creation.md)) or use the Schema Generation endpoint (see above).

    ### Inputs

    | Parameters    | Usage                                   | Example values               | Definition            |
    |---------------|-----------------------------------------|------------------------------|-----------------------|
    | schema        | JSON request body                       | see below                    | the schema definition |

    #### Domain and dataset

    The domain and dataset names must adhere to the following conditions:

    - Only alphanumeric characters allowed
    - Have to start with an alphabetic character

    ### Accepted permissions

    In order to use this endpoint you need the `DATA_ADMIN` permission.

    ### Click  `Try it out` to use the endpoint
    """
    try:
        schema_file_name = data_service.upload_schema(schema)
        return {"details": schema_file_name}
    except (CrawlerCreationError, CrawlerAlreadyExistsError) as error:
        _delete_uploaded_schema(schema)
        raise error


@schema_router.put(
    "",
    status_code=http_status.HTTP_200_OK,
    dependencies=[Security(secure_endpoint, scopes=[Action.DATA_ADMIN.value])],
)
async def update_schema(schema: Schema):
    """
    ## Update Schema

    This endpoint is for uploading an updated schema definition. This will allow you to subsequently upload
    datasets that match the updated schema. To create a schema definition (see
    the [schema writing guide](https://github.com/no10ds/rapid-api/blob/main/docs/guides/usage/schema_creation.md)) or use the Schema Generation endpoint (see above).

    ### Inputs

    | Parameters    | Usage                                   | Example values               | Definition            |
    |---------------|-----------------------------------------|------------------------------|-----------------------|
    | schema        | JSON request body                       | see below                    | the schema definition |

    #### Domain and dataset

    The domain and dataset names must match the original schema that is being updated

    #### Schema metadata

    Metadata information for an updated schema will be taken from the original schema

    ### Accepted permissions

    In order to use this endpoint you need the `DATA_ADMIN` permission.

    ### Click  `Try it out` to use the endpoint
    """
    schema_file_name = data_service.update_schema(schema)
    return {"details": schema_file_name}


def _delete_uploaded_schema(schema: Schema):
    delete_service.delete_schema(
        schema.get_domain(),
        schema.get_dataset(),
        schema.get_sensitivity(),
        schema.get_version(),
    )


def _log_and_raise_error(log_message: str, error_message: str):
    AppLogger.error(f"{log_message}: {error_message}")
    raise AWSServiceError(message=error_message)
