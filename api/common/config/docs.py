import os

from fastapi.openapi.utils import get_openapi

VERSION = os.getenv("VERSION", None)
COMMIT_SHA = os.getenv("COMMIT_SHA", None)

RAPID_DESCRIPTION = """
See the full [changelog here](https://github.com/no10ds/rapid-api/blob/master/changelog.md)

### rAPId usage [guide](https://github.com/no10ds/rapid-api/blob/master/docs/usage/usage.md)

### Create schema [documentation](https://github.com/no10ds/rapid-api/blob/master/docs/usage/schema_creation.md)

### ADR [documentation](https://github.com/no10ds/rapid-api/blob/master/docs/architecture/adr/0001-query-endpoint.md)

"""

RAPID_TAGS = [
    {
        "name": "Status",
        "description": "Shows current status of application, version and commit sha.",
    },
    {
        "name": "Schema",
        "description": "Manage schema generation and upload.",
    },
    {
        "name": "Datasets",
        "description": "Manage dataset upload and querying.",
    },
    {
        "name": "Client",
        "description": "Manage client creation.",
    },
    {
        "name": "Protected Domains",
        "description": "Manage protected domains",
    },
]


def custom_openapi_docs_generator(app):
    def custom_openapi_docs():
        if app.openapi_schema:
            return app.openapi_schema
        openapi_schema = get_openapi(
            title="rAPId",
            version=VERSION if VERSION is not None else "DEV",
            description=RAPID_DESCRIPTION,
            routes=app.routes,
            tags=RAPID_TAGS,
        )
        app.openapi_schema = openapi_schema
        return app.openapi_schema

    return custom_openapi_docs
