"""
This script migrates all of your current rAPId datasets to a given Layer, if none is given then it will just be the default layer.

The resources that get changed are the:
- Data and Schemas in S3
- Glue tables
- Glue crawlers

Please ensure that none of the crawlers are running when you start this script
"""
import argparse
import os
import json
from typing import List

import boto3
import dotenv

dotenv.load_dotenv()

AWS_REGION = os.environ["AWS_REGION"]
DATA_BUCKET = os.environ["DATA_BUCKET"]
RESOURCE_PREFIX = os.environ["RESOURCE_PREFIX"]
DATA_PATH = "data"
SCHEMAS_PATH = "data/schemas"
RAW_DATA_PATH = "raw_data"
GLUE_DB = f"{RESOURCE_PREFIX}_catalogue_db"


def main(layer: str, s3_client, glue_client, resource_client):
    migrate_files(layer, s3_client)
    migrate_crawlers(layer, glue_client, resource_client)


def migrate_crawlers(layer: str, glue_client, resource_client):
    """
    Steps:
    1. Fetch all of the crawlers and their tags
    2. Get the configuration for each crawler, add the layer changes and create the new one
    3. Delete the old crawler and table
    4. Run the new crawler
    """
    print("- Starting to migrate the crawlers")
    crawlers = fetch_all_crawlers(resource_client)
    tables = fetch_all_tables(glue_client)

    for crawler in crawlers:
        # Don't recreate crawlers if have already been migrated - can happen if the script is run twice
        if not crawler["Name"].startswith(f"{RESOURCE_PREFIX}_crawler/{layer}"):
            print(f"-- Migrating the crawler: {crawler['Name']}")
            # Create new crawler
            new_crawler = create_new_crawler(layer, crawler, glue_client)

            # Delete old table and crawler
            table_prefix = glue_client.get_crawler(Name=crawler["Name"])["Crawler"][
                "TablePrefix"
            ]
            tables_to_delete = [
                table["Name"]
                for table in tables
                if table["Name"].startswith(table_prefix)
            ]
            for table in tables_to_delete:
                glue_client.delete_table(Name=table, DatabaseName=GLUE_DB)

            glue_client.delete_crawler(Name=crawler["Name"])

            glue_client.start_crawler(Name=new_crawler)
    print("- Finished migrating the crawlers")


def format_tags_acceptably_for_crawler_creation(tags: List[dict]) -> dict:
    return {tag["Key"]: tag["Value"] for tag in tags}


def create_new_crawler(layer: str, crawler_info: dict, glue_client) -> str:
    res = glue_client.get_crawler(Name=crawler_info["Name"])
    new_crawler = res["Crawler"]
    # Add layer to the name
    new_crawler["Name"] = new_crawler["Name"].replace(
        f"{RESOURCE_PREFIX}_crawler", f"{RESOURCE_PREFIX}_crawler/{layer}"
    )
    # Add layer to table prefix
    new_crawler["TablePrefix"] = f"{layer}_{new_crawler['TablePrefix']}"
    # Increment TableLevelConfiguration level to 6
    new_crawler[
        "Configuration"
    ] = """{"Version":1.0,"Grouping":{"TableGroupingPolicy":"CombineCompatibleSchemas","TableLevelConfiguration":6}}"""
    # Add layer to the target path
    s3_target = new_crawler["Targets"]["S3Targets"][0]
    s3_target["Path"] = s3_target["Path"].replace(
        f"/{DATA_PATH}", f"/{DATA_PATH}/{layer}"
    )

    # Delete unacceptable attributes
    [
        delete_crawler_attribute(new_crawler, attribute)
        for attribute in [
            "State",
            "CrawlElapsedTime",
            "CreationTime",
            "LastUpdated",
            "LastCrawl",
            "Version",
        ]
    ]

    # Add tags
    new_crawler["Tags"] = format_tags_acceptably_for_crawler_creation(
        crawler_info["Tags"]
    )
    glue_client.create_crawler(**new_crawler)

    return new_crawler["Name"]


def delete_crawler_attribute(crawler, attribute):
    try:
        del crawler[attribute]
    # Fail gracefully, it is fine if the attribute is not present, we were deleting it anyway
    except KeyError:
        pass


def fetch_all_tables(glue_client):
    paginator = glue_client.get_paginator("get_tables")
    page_iterator = paginator.paginate(DatabaseName=GLUE_DB)
    return [item for page in page_iterator for item in page["TableList"]]


def fetch_all_crawlers(resource_client):
    paginator = resource_client.get_paginator("get_resources")
    page_iterator = paginator.paginate(ResourceTypeFilters=["glue:crawler"])
    resources = [
        item for page in page_iterator for item in page["ResourceTagMappingList"]
    ]
    crawlers = [
        {**resource, **{"Name": resource["ResourceARN"].split(":crawler/")[-1]}}
        for resource in resources
        if f":crawler/{RESOURCE_PREFIX}_crawler/" in resource["ResourceARN"]
    ]
    return crawlers


def migrate_files(layer: str, s3_client):
    print("- Migrating the files")
    # Schemas
    move_files_by_prefix(s3_client, "data/schemas", f"schemas/{layer}")
    add_layer_to_schemas(s3_client, layer, f"schemas/{layer}")

    # Data
    move_files_by_prefix(s3_client, "data", f"data/{layer}")

    # Raw data
    move_files_by_prefix(s3_client, "raw_data", f"raw_data/{layer}")
    print("- Finished migrating the files")


def add_layer_to_schemas(s3_client, layer: str, path: str):
    print("-- Adding layer to the schemas")
    paginator = s3_client.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=DATA_BUCKET, Prefix=path)
    files = [item for page in page_iterator for item in page["Contents"]]
    for file in files:
        res = s3_client.get_object(Bucket=DATA_BUCKET, Key=file["Key"])
        json_object = json.loads(res["Body"].read().decode("utf-8"))
        json_object["metadata"] = {"layer": layer, **json_object["metadata"]}
        s3_client.put_object(
            Body=json.dumps(json_object), Bucket=DATA_BUCKET, Key=file["Key"]
        )


def move_files_by_prefix(s3_client, src_prefix: str, dest_prefix: str):
    print(f"-- Moving files from the prefix [{src_prefix}] to [{dest_prefix}]")
    paginator = s3_client.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=DATA_BUCKET, Prefix=src_prefix)

    try:
        files = [item for page in page_iterator for item in page["Contents"]]
    except KeyError:
        print(f"-- There were no files in the prefix [{src_prefix}] to migrate")
        return None

    for file in files:
        src_key = file["Key"]
        # Don't move files if they are already in the destination - can happen if the script is run twice
        if not src_key.startswith(dest_prefix):
            dest_key = src_key.replace(src_prefix, dest_prefix)
            copy_source = {"Bucket": DATA_BUCKET, "Key": src_key}
            s3_client.copy_object(
                Bucket=DATA_BUCKET,
                CopySource=copy_source,
                Key=dest_key,
            )

            s3_client.delete_object(Bucket=DATA_BUCKET, Key=src_key)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--layer",
        help="Specify the layer to migrate the resources to. Defaults to 'default'",
    )
    args = parser.parse_args()
    if args.layer:
        layer = args.layer
    else:
        layer = "default"

    print(f"Migration to layer [{layer}] starting")
    s3_client = boto3.client("s3")
    glue_client = boto3.client("glue", region_name=AWS_REGION)
    resource_client = boto3.client("resourcegroupstaggingapi", region_name=AWS_REGION)

    main(layer, s3_client, glue_client, resource_client)
    print("Migration finished")
