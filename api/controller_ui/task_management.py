import os
from typing import List, Dict

from fastapi import APIRouter, Security
from fastapi import Request
from fastapi.templating import Jinja2Templates

from api.application.services.authorisation.authorisation_service import (
    secure_endpoint,
)
from api.application.services.authorisation.token_utils import parse_token
from api.application.services.job_service import JobService
from api.common.config.auth import Action, RAPID_ACCESS_TOKEN
from api.common.config.constants import BASE_API_PATH

jobs_ui_router = APIRouter(
    prefix=f"{BASE_API_PATH}/tasks",
    responses={404: {"description": "Not found"}},
    include_in_schema=False,
)

templates = Jinja2Templates(directory=(os.path.abspath("templates")))

job_service = JobService()


@jobs_ui_router.get(
    "",
    dependencies=[
        Security(
            secure_endpoint,
            scopes=[Action.READ.value, Action.WRITE.value],
        )
    ],
)
def jobs_overview(request: Request):
    subject_id = parse_token(request.cookies.get(RAPID_ACCESS_TOKEN)).subject
    jobs = sorted(
        job_service.get_all_jobs(subject_id),
        key=lambda job: (
            job.get("status", ""),
            job.get("type", ""),
            job.get("domain", ""),
            job.get("dataset", ""),
            job.get("version", ""),
        ),
    )

    return templates.TemplateResponse(
        name="jobs.html", context={"request": request, "jobs": format_jobs_for_ui(jobs)}
    )


@jobs_ui_router.get(
    "/{job_id}",
    dependencies=[
        Security(
            secure_endpoint,
            scopes=[Action.READ.value, Action.WRITE.value],
        )
    ],
)
def job_details(request: Request, job_id: str):
    job = job_service.get_job(job_id)

    return templates.TemplateResponse(
        name="job_detail.html",
        context={"request": request, "job": format_job_for_ui(job)},
    )


def format_jobs_for_ui(jobs: List[Dict]) -> List[Dict]:
    return [format_job_for_ui(job) for job in jobs]


def format_job_for_ui(job: Dict) -> Dict:
    return {**job, "step": job["step"].replace("_", " ").lower().capitalize()}
