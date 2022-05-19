from fastapi import APIRouter
from fastapi import Security
from fastapi import status as http_status

from api.application.services.authorisation_service import protect_endpoint
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
    dependencies=[Security(protect_endpoint, scopes=[Action.DATA_ADMIN.value])],
)
def create_protected_domain(domain: str):
    protected_domain_service.create_scopes(domain)
    return f"Successfully created protected domain for {domain}"


@protected_domain_router.get(
    "",
    dependencies=[Security(protect_endpoint, scopes=[Action.DATA_ADMIN.value])],
)
def list_protected_domains():
    return protected_domain_service.list_domains()
