from fastapi import APIRouter
from fastapi import Security
from fastapi import status as http_status

from api.application.services.authorisation.authorisation_service import (
    secure_endpoint,
)
from api.application.services.job_service import JobService
from api.common.config.auth import Action

jobs_service = JobService()

jobs_router = APIRouter(
    prefix="/jobs",
    tags=["Jobs"],
    responses={404: {"description": "Not found"}},
)


@jobs_router.get(
    "",
    dependencies=[Security(secure_endpoint, scopes=[Action.WRITE.value])],
    status_code=http_status.HTTP_200_OK,
)
async def list_all_jobs():
    """
    ## List all jobs

    Use this endpoint to retrieve a list of all currently tracked asynchronous processing jobs.

    ### Accepted permissions

    You will always be able to list all jobs, provided you have
    a `WRITE` permission, e.g.: `WRITE_ALL`, `WRITE_PUBLIC`, `WRITE_PRIVATE`, `WRITE_PROTECTED_{DOMAIN}`

    ### Click  `Try it out` to use the endpoint

    """
    return jobs_service.get_all_jobs()
