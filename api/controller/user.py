from fastapi import APIRouter
from fastapi import Security
from fastapi import status as http_status

from api.application.services.authorisation.authorisation_service import secure_endpoint
from api.application.services.subject_service import SubjectService
from api.common.config.auth import Action
from api.domain.user import UserRequest

subject_service = SubjectService()

user_router = APIRouter(
    prefix="/user",
    tags=["User"],
    responses={404: {"description": "Not found"}},
)


@user_router.post(
    "",
    status_code=http_status.HTTP_201_CREATED,
    dependencies=[Security(secure_endpoint, scopes=[Action.USER_ADMIN.value])],
)
async def create_user(user_request: UserRequest):
    """
    ### Inputs

    | Parameters       | Usage               | Example values   | Definition                                                                |
    |------------------|---------------------|------------------|---------------------------------------------------------------------------|
    | `client details` | JSON Request Body   | See below        | The name of the client application to onboard and the granted permissions |

    ```json
    {
    "client_name": "department_for_education",
    "permissions": [
        "READ_ALL",
        "WRITE_PUBLIC"
    ]
    }
    ```

    ### Client Name

    The client name must adhere to the following conditions:

    - Alphanumeric
    - Start with an alphabetic character
    - Can contain any symbol of `. - _ @`
    - Must be between 3 and 128 characters

    #### Permissions you can grant to the client

    Depending on what permission you would like to grant the onboarding client, the relevant permission(s) must be assigned.
    Available choices are:

    - `READ_ALL` - allow client to read any dataset
    - `READ_PUBLIC` - allow client to read any public dataset
    - `READ_PRIVATE` - allow client to read any dataset with sensitivity private or public
    - `READ_PROTECTED_{DOMAIN}` - allow client to read datasets within a specific protected domain
    - `WRITE_ALL` - allow client to write any dataset
    - `WRITE_PUBLIC` - allow client to write any public dataset
    - `WRITE_PRIVATE` - allow client to write any dataset with sensitivity private or public
    - `WRITE_PROTECTED_{DOMAIN}` - allow client to write datasets within a specific protected domain
    - `DATA_ADMIN` - allow client to add a schema for a dataset of any sensitivity
    - `USER_ADMIN` - allow client to add a new client

    The protected domains can be listed [here](#Protected%20Domains/list_protected_domains_protected_domains_get) or created [here](#Protected%20Domains/create_protected_domain_protected_domains__domain__post).

    ### Click  `Try it out` to use the endpoint

    """
    user_response = subject_service.create_user(user_request)
    return user_response
