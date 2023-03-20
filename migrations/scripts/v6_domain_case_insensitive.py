import os
import dotenv
import boto3

dotenv.load_dotenv()

AWS_REGION = os.environ["AWS_REGION"]
DATA_BUCKET = os.environ["DATA_BUCKET"]
RESOURCE_NAME_PREFIX = os.environ["RESOURCE_PREFIX"]

s3_client = boto3.client("s3")
glue_client = boto3.client("glue", region_name=AWS_REGION)


def get_raw_full_path(prefix):
    item = s3_client.list_objects_v2(Bucket=DATA_BUCKET, Prefix=f"data/{prefix}")
    contents = item["Contents"]
    return contents[0]["Key"]


def get_lower_full_path(prefix):
    item = s3_client.list_objects_v2(Bucket=DATA_BUCKET, Prefix=f"data/{prefix}")
    contents = item["Contents"]
    splits = contents[0]["Key"].split("/")
    return f"{splits[0]}/{splits[1].lower()}/{splits[2]}/{splits[3]}/{splits[4]}"


def move_s3_object(path_raw, path_lower):
    result = s3_client.copy_object(
        Bucket=DATA_BUCKET, CopySource=f"{DATA_BUCKET}/{path_raw}", Key=f"{path_lower}"
    )
    return result


def remove_s3_object(path):
    result = s3_client.delete_object(Bucket=DATA_BUCKET, Key=f"{DATA_BUCKET}/{path}")
    return result


def delete_crawler(resource_name_prefix, raw_name):
    try:
        splits = raw_name.split("/")
        name = f"{resource_name_prefix}_crawler/{splits[0]}/{splits[1]}"
        result = glue_client.delete_crawler(Name=name)
    except Exception:
        print("Skipping crawler deletion")
        return None
    return result


def retrieve_all_raw():
    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=DATA_BUCKET, Prefix="data/")
    loop_map_raw = {}

    for page in pages:
        contents = page["Contents"]

        for item in contents:
            key = item["Key"]
            last_modified = item["LastModified"]

            # We do not worry about the schemas
            if not key.startswith("data/schemas"):
                splits = key.split("/")
                domain = splits[1]
                dataset = splits[2]
                version = splits[3]

                if version == "None":
                    break

                raw_key = f"{domain}/{dataset}/{version}"
                loop_map_raw[raw_key] = last_modified

    return loop_map_raw


def retrieve_move_delete(loop_map_raw):
    to_delete = []
    to_move = []

    for key in loop_map_raw:
        key_splits = key.split("/")
        key_lower = f"{key_splits[0].lower()}/{key_splits[1]}/{key_splits[2]}"

        last_modified = loop_map_raw[key]
        if (key != key_lower) and (key_lower in loop_map_raw):
            match_last_modified = loop_map_raw[key_lower]

            # If the lowercase key last modified date is more recent than the
            # uppercase - we remove the uppercase version
            if match_last_modified > last_modified:
                to_delete.append(key)

            # Otherwise we want to move the uppecase
            else:
                to_move.append(key)

        # The uppercase does not have a lower case version and therefore needs to be moved
        elif key != key_lower:
            to_move.append(key)

    return to_delete, to_move


def list_crawlers():
    crawlers = []
    next_page = ""
    while True:
        response = glue_client.list_crawlers(NextToken=next_page)
        current_crawlers = [
            crawler
            for crawler in response["CrawlerNames"]
            if crawler.startswith(f"{RESOURCE_NAME_PREFIX}_crawler/")
        ]
        crawlers += current_crawlers
        next_page = response.get("NextToken")

        if next_page is None:
            break

    return crawlers


def edit_crawlers():
    crawlers = list_crawlers()
    for crawler in crawlers:
        print(f"Updating crawler {crawler}")
        glue_client.update_crawler(
            Name=crawler, SchemaChangePolicy={"DeleteBehavior": "DELETE_FROM_DATABASE"}
        )


loop_map_raw = retrieve_all_raw()
to_delete, to_move = retrieve_move_delete(loop_map_raw)

try:

    for key in to_move:
        key_raw = get_raw_full_path(key)
        key_lower = get_lower_full_path(key)

        print(f"Moving item {key_raw} to new item {key_lower}")
        move_s3_object(key_raw, key_lower)

        print("Deleting crawler")
        delete_crawler(RESOURCE_NAME_PREFIX, key)

        print(f"Deleting old file {key_raw}")
        remove_s3_object(key_raw)

    for key in to_delete:
        key_raw = get_raw_full_path(key)
        key_lower = get_lower_full_path(key)

        print("Deleting crawler")
        delete_crawler(RESOURCE_NAME_PREFIX, key)

        print(f"Deleting old file {key_raw}")
        remove_s3_object(key_raw)

    edit_crawlers()

except Exception as e:
    print(e)
