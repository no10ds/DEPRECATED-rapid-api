import os
import re
import sys
from functools import reduce

import boto3
from boto3.dynamodb.conditions import Key, Attr, Or
from botocore.exceptions import ClientError

AWS_REGION = os.environ["AWS_REGION"]
RESOURCE_PREFIX = os.environ["RESOURCE_PREFIX"]
DATA_BUCKET = os.environ["DATA_BUCKET"]
DYNAMO_PERMISSIONS_TABLE_NAME = f"{RESOURCE_PREFIX}_users_permissions"
PROTECTED_DOMAIN_PERMISSIONS_PARAMETER_NAME = (
    f"{RESOURCE_PREFIX}_protected_domain_permissions"
)

database = boto3.resource("dynamodb", region_name=AWS_REGION).Table(
    DYNAMO_PERMISSIONS_TABLE_NAME
)
s3_client = boto3.client("s3")
glue_client = boto3.client("glue", region_name=AWS_REGION)
s3_bucket = DATA_BUCKET
protected_path = "data/schemas/PROTECTED/"


def delete_protected_domain(domain: str):
    # DELETE DATASET FILES
    print("Deleting files...")
    delete_data(domain)

    # DELETE EVIDENCE IN DATABASE AND SSM
    print("Deleting permissions...")
    delete_permission_from_db(domain)


def delete_data(domain):
    # Check protected domain exists
    metadatas = find_protected_datasets(domain)
    if metadatas:
        # Delete all files and raw files for each domain/dataset
        for metadata in metadatas:
            try:
                # Delete crawler
                glue_client.delete_crawler(
                    Name=f'{RESOURCE_PREFIX}_crawler/{metadata["domain"]}/{metadata["dataset"]}'
                )

                # List of related files
                data_files = get_file_paths(
                    domain=metadata["domain"], dataset=metadata["dataset"], path="data"
                )
                raw_data_files = get_file_paths(
                    domain=metadata["domain"],
                    dataset=metadata["dataset"],
                    path="raw_data",
                )

                # Delete raw files
                if raw_data_files:
                    delete_s3_files(raw_data_files)

                # Delete files and related tables
                if data_files:
                    delete_s3_files(data_files)
                    # Delete table
                    glue_client.delete_table(
                        DatabaseName=f"{RESOURCE_PREFIX}_catalogue_db",
                        Name=f'{metadata["domain"]}_{metadata["dataset"]}',
                    )
                # Delete schema
                delete_s3_files(
                    [
                        {
                            "Key": f'{protected_path}{metadata["domain"]}-{metadata["dataset"]}.json'
                        }
                    ]
                )
            except ClientError as error:
                print(
                    f'Unable to delete data related to {metadata["domain"]}-{metadata["dataset"]}'
                )
                print(error)
            print(f'Deleting files in {metadata["domain"]}/{metadata["dataset"]}')


def find_protected_datasets(domain):
    response = s3_client.list_objects(
        Bucket=s3_bucket,
        Prefix=protected_path,
    )
    if response.get("Contents") is not None:
        protected_datasets = [
            path["Key"]
            for path in response["Contents"]
            if path["Key"].startswith(f"{protected_path}{domain}")
        ]
        data = []
        for path in protected_datasets:
            dataset = re.search(f"{protected_path}{domain}-([a-z_]+).json", path).group(
                1
            )
            data.append({"domain": domain, "dataset": dataset})
        return data
    else:
        return None


def get_file_paths(domain: str, dataset: str, path: str):
    response = s3_client.list_objects(
        Bucket=s3_bucket,
        Prefix=f"{path}/{domain}/{dataset}",
    )
    if response.get("Contents") is not None:
        return [{"Key": path["Key"]} for path in response["Contents"]]


def delete_s3_files(file_path):
    s3_client.delete_objects(
        Bucket=s3_bucket,
        Delete={"Objects": file_path},
    )


def delete_permission_from_db(domain):
    # Get permission ids
    protected_permissions = get_protected_permissions(domain)

    if protected_permissions:
        # Check subjects with permission
        subjects_with_protected_permissions = get_users_with_protected_permissions(
            protected_permissions
        )

        # Delete permissions for users with protected domain and protected domain
        remove_protected_permissions_from_db(
            protected_permissions, subjects_with_protected_permissions
        )


def get_protected_permissions(domain):
    response = database.query(
        KeyConditionExpression=Key("PK").eq("PERMISSION"),
        FilterExpression=Attr("Domain").contains(domain.upper()),
    )
    protected_permissions = [item["SK"] for item in response["Items"]]
    return protected_permissions


def get_users_with_protected_permissions(protected_permissions):
    subjects_with_protected_permissions = database.query(
        KeyConditionExpression=Key("PK").eq("SUBJECT"),
        FilterExpression=reduce(
            Or,
            ([Attr("Permissions").contains(value) for value in protected_permissions]),
        ),
    )["Items"]
    return subjects_with_protected_permissions


def remove_protected_permissions_from_db(
    protected_permissions, subjects_with_protected_permissions
):
    try:
        with database.batch_writer() as batch:
            for subject in subjects_with_protected_permissions:
                subject["Permissions"].difference_update(protected_permissions)

                if len(subject["Permissions"]) == 0:
                    subject["Permissions"] = {}

                batch.put_item(Item=subject)
            for permission in protected_permissions:
                batch.delete_item(
                    Key={
                        "PK": "PERMISSION",
                        "SK": permission,
                    }
                )
    except ClientError as error:
        print(f"Unable to delete {protected_permissions} from db")
        print(error)


if __name__ == "__main__":
    if len(sys.argv) == 2:
        delete_protected_domain(sys.argv[1])
    else:
        print("This method requires the domain for the protected domain to be deleted.")
        print(f"E.g. python {os.path.basename(__file__)} <domain>")
