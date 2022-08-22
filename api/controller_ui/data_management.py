import os
from typing import List

from dateutil import parser
from fastapi import APIRouter, Security
from fastapi import Request
from fastapi.templating import Jinja2Templates

from api.application.services.authorisation.authorisation_service import (
    RAPID_ACCESS_TOKEN,
    secure_endpoint,
    secure_dataset_endpoint,
)
from api.application.services.authorisation.token_utils import parse_token
from api.application.services.data_service import DataService
from api.application.services.dataset_service import DatasetService
from api.common.config.auth import Action

data_management_router = APIRouter(
    prefix="", responses={404: {"description": "Not found"}}, include_in_schema=False
)

templates = Jinja2Templates(directory=(os.path.abspath("templates")))

upload_service = DatasetService()
data_service = DataService()


def group_datasets_by_domain(datasets: List[str]):
    grouped_datasets = {}
    for dataset in datasets:
        domain, dataset = dataset.split("/")[0], dataset.split("/")[1]
        if domain not in grouped_datasets:
            grouped_datasets[domain] = [dataset]
        else:
            grouped_datasets[domain].append(dataset)
    return grouped_datasets


@data_management_router.get(
    "/download", dependencies=[Security(secure_endpoint, scopes=[Action.READ.value])]
)
def select_dataset(request: Request):
    subject_id = parse_token(request.cookies.get(RAPID_ACCESS_TOKEN)).subject
    datasets = upload_service.get_authorised_datasets(subject_id, Action.READ)

    grouped_datasets = group_datasets_by_domain(datasets)

    return templates.TemplateResponse(
        name="datasets.html", context={"request": request, "datasets": grouped_datasets}
    )


@data_management_router.get(
    "/download/{domain}/{dataset}",
    dependencies=[Security(secure_dataset_endpoint, scopes=[Action.READ.value])],
)
def download_dataset(request: Request, domain: str, dataset: str):
    dataset_info = data_service.get_dataset_info(domain, dataset)
    date = parser.parse(dataset_info.metadata.last_updated)
    new_date = date.strftime("%-d %b %Y at %H:%M:%S")
    columns = []

    for column in dataset_info.columns:
        columns.append(
            {
                "name": column.name,
                "data_type": column.data_type,
                "allow_null": column.allow_null,
                "min": column.statistics["min"] if column.statistics else "-",
                "max": column.statistics["max"] if column.statistics else "-",
            }
        )

    ui_dataset_info = {
        "domain": domain,
        "dataset": dataset,
        "number_of_rows": dataset_info.metadata.number_of_rows,
        "number_of_columns": dataset_info.metadata.number_of_columns,
        "last_updated": new_date,
        "columns": columns,
    }

    return templates.TemplateResponse(
        name="download.html",
        context={"request": request, "dataset_info": ui_dataset_info},
    )


@data_management_router.get(
    "/upload", dependencies=[Security(secure_endpoint, scopes=[Action.WRITE.value])]
)
def upload(request: Request):
    subject_id = parse_token(request.cookies.get(RAPID_ACCESS_TOKEN)).subject
    datasets = upload_service.get_authorised_datasets(subject_id, Action.WRITE)

    grouped_datasets = group_datasets_by_domain(datasets)

    return templates.TemplateResponse(
        name="upload.html", context={"request": request, "datasets": grouped_datasets}
    )
