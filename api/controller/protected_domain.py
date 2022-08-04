from fastapi import APIRouter
from fastapi import Security
from fastapi import status as http_status

from api.application.services.authorisation.authorisation_service import secure_endpoint
from api.application.services.protected_domain_service import ProtectedDomainService
from api.common.config.auth import Action

protected_domain_service = ProtectedDomainService()

protected_domain_router = APIRouter(
    prefix="/protected_domains",
    tags=["Protected Domains"],
    responses={404: {"description": "Not found"}},
)


@protected_domain_router.post(
    "/{domain}",
    status_code=http_status.HTTP_201_CREATED,
    dependencies=[Security(secure_endpoint, scopes=[Action.DATA_ADMIN.value])],
)
def create_protected_domain(domain: str):
    """
    ## Create protected domain

    Protected domains can be created to restrict access permissions to specific domains

    Use this endpoint to create a new protected domain. After this you can create clients with the scope for this domain and create `PROTECTED` datasets within this domain.


    ### Inputs

    | Parameters       | Usage               | Example values   | Definition                       |
    |------------------|---------------------|------------------|----------------------------------|
    | `domain`         | URL Parameter       | `land`           | The name of the protected domain |

    ### Domain

    The domain name must adhere to the following conditions:

    - Alphanumeric
    - Start with an alphabetic character
    - Can contain any symbol of `- _`

    ### Accepted permissions

    In order to use this endpoint you need the `DATA_ADMIN` scope

    ### Click  `Try it out` to use the endpoint
    """
    protected_domain_service.create_protected_domain_permission(domain)
    return f"Successfully created protected domain for {domain}"


@protected_domain_router.get(
    "",
    dependencies=[Security(secure_endpoint, scopes=[Action.DATA_ADMIN.value])],
)
def list_protected_domains():
    """
    ## List protected domains

    Use this endpoint to list the protected domains that currently exist.

    ### Outputs

    List of protected scopes in json format in the response body:

    ```json
    [
    "land",
    "department"
    ]
    ```
    ### Accepted permissions

    In order to use this endpoint you need the `DATA_ADMIN` permission

    ### Click  `Try it out` to use the endpoint
    """
    return protected_domain_service.list_protected_domains()
