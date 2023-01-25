from jinja2 import Template
from typing import List

from api.common.config.aws import RESOURCE_PREFIX, GLUE_CATALOGUE_DB_NAME


DATASET_COLUMN = "dataset"
DATA_COLUMN = "data"
DATA_TYPE_COLUMN = "data_type"


METADATA_QUERY = Template(
    f"""
SELECT * FROM (
    SELECT 
        metadata.dataset as {DATASET_COLUMN},
        "column".name as {DATA_COLUMN},
        'column_name' as {DATA_TYPE_COLUMN}
    FROM "{GLUE_CATALOGUE_DB_NAME}"."{RESOURCE_PREFIX}_metadata_table"
    CROSS JOIN UNNEST("columns") AS t ("column")
    UNION ALL
    SELECT 
        metadata.dataset as {DATASET_COLUMN},
        metadata.description as {DATA_COLUMN},
        'description' as {DATA_TYPE_COLUMN}
    FROM "{GLUE_CATALOGUE_DB_NAME}"."{RESOURCE_PREFIX}_metadata_table"
    UNION ALL
    SELECT
        metadata.dataset as {DATASET_COLUMN},
        metadata.dataset as {DATA_COLUMN},
        'dataset_name' as {DATA_TYPE_COLUMN}
    FROM "dev-rapid-no10ds_catalogue_db"."dev-rapid-no10ds_metadata_table"
)
WHERE {{{{ where_clause }}}}
"""
)


def generate_where_clause(search_term: str) -> List[str]:
    return " OR ".join(
        [
            f"lower({DATA_COLUMN}) LIKE '%{word.lower()}%'"
            for word in search_term.split(" ")
        ]
    )


def metadata_search_query(search_term: str) -> str:
    where_clause = generate_where_clause(search_term)
    return METADATA_QUERY.render(where_clause=where_clause)
