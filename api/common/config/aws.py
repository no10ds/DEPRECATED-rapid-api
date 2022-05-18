import os

DATA_BUCKET = os.environ["DATA_BUCKET"]
AWS_ACCOUNT = os.environ["AWS_ACCOUNT"]
RESOURCE_PREFIX = os.environ["RESOURCE_PREFIX"]
DOMAIN_NAME = os.environ["DOMAIN_NAME"]
AWS_REGION = os.environ["AWS_REGION"]

OUTPUT_QUERY_BUCKET = "aws-athena-query-results-" + AWS_ACCOUNT + "-" + AWS_REGION
GLUE_CATALOGUE_DB_NAME = RESOURCE_PREFIX + "_catalogue_db"
ATHENA_DATABASE = GLUE_CATALOGUE_DB_NAME
ATHENA_WORKGROUP = RESOURCE_PREFIX + "_athena_workgroup"
GLUE_CRAWLER_ROLE = "glue_services_access"
GLUE_CONNECTION_NAME = "s3-network-connection"

SCHEMAS_LOCATION = "data/schemas"

MAX_CUSTOM_TAG_COUNT = 30

GLUE_CSV_SERIALISATION_LIBRARY = "org.apache.hadoop.hive.serde2.OpenCSVSerde"
GLUE_QUOTE_CHAR = '"'
GLUE_TABLE_PRESENCE_CHECK_RETRY_COUNT = 18
GLUE_TABLE_PRESENCE_CHECK_INTERVAL = 20

INFERRED_UNNAMED_COLUMN_PREFIX = (
    "unnamed_"  # Pandas infers an empty column name as "unnamed_\d"
)
