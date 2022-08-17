# Future Features

This section describes improvements that have been suggested as the rAPId service evolves.

## Dataset Versioning

### Problem

The users need to be able to upload/query previous versions of the dataset. We need to keep track of what was modified
in the schema whilst maintaining access to previous versions for querying and uploading.

### Suggested Solution

Click [here](diagrams/dataset-versioning.png) to see the diagram of the solution

We can store the different schema versions of the same domain/dataset where previous files will contain a suffix of the
version. The latest schema file should not contain a version.

Example:

```
schemas/
    {domain}_{dataset}-1.1.json
    {domain}_{dataset}-1.2.json
    {domain}_{dataset}-1.3.json
    {domain}_{dataset}.json
```

For each dataset schema version, there will be a corresponding location for the data in s3

Example:

```
data/
    {domain}/{dataset}-1.1/<data_files>
    {domain}/{dataset}-1.2/<data_files>
    {domain}/{dataset}-1.3/<data_files>
    {domain}/{dataset}-1.4/<data_files>
```

This way the user would be able to upload data to the latest version (or older versions) if required. At the same time
the user would be able to query the latest and previous versions of the dataset.

Additionally, the query endpoint can be utilised when the version is omitted. It will instead query the latest version
which can be retrieved from the latest schema. This should be easy to implement because currently we retrieve the schema
as part of the request flows and so can easily retrieve the version number from its metadata.

### Considerations

#### User vs. Programmatic defined versions

Allowing the user to define the version number provides more flexibility and power to them in defining what a version
increase means. e.g.: If this is a minor version change or a more major alteration.

However, this also comes with some substantial drawbacks.

Beyond the obvious inconsistency across different datasets, more validation would also need to be in place, including:

- Checking the new version is greater than the latest one
- Checking the new version matches the existing specified format
- Checking the new version is not wildly ahead of the previous version (e.g.: v.1.0.0 -> v.53.4.9 makes no sense)

On the other hand, having the service determine to what to increment the next version, avoids these issues and leads to
a more uniform application of schema versioning.

### Assumptions

- A department is unable to update an existing schema in place

### Changes to existing endpoints

The following endpoints need to be created or changed to support access to data versioning by version parameter

| Endpoint        | URL                                                        |
|-----------------|------------------------------------------------------------|
| Generate Schema | `POST /schema/{domain}/{dataset}/{sensitivity}/{version}`  |
| Upload Schema   | This only changes internally, read upload schema below.    |
| Query Data      | `POST /datasets/{domain}/{dataset}/{version}/query`        |
| Upload Data     | `POST /datsets/{domain}/{dataaset}/{version}`              |
| Info            | `GET /datasets/{domain}/{dataset}/{version}/info`          |
| List Dataset    | Should also include datasets versions.                     |
| List Raw Files  | `GET /datasets/{domain}/{dataset}/{version}/files`         |
| Delete Files    | `DELETE /datasets/{domain}/{dataset}/{version}/{filename}` |

If version is omitted, then the latest version is used instead.

#### Upload Schema

*Metadata*

When a user uploads a schema they would also have to include a few more keys which would be `version` and `description`.

`version` - This would be user defined and has to be unique, since previously uploaded versions cannot be overridden

`description` - This would have to be specified so that upon uploading data it would be easy for a user to understand
the difference for this version

Example:

```
{
  "metadata": {
    "domain": "land",
    "dataset": "train_journeys",
    "version": "1"
    "description": "A new column, cities, has been added with a datatype of string"
    ...
  },
  "columns": [
    ...
  ]
}
```

## Change storage data format

### Problem

Storing files in CSV format might be less efficient than parquet, therefore transforming to parquet can be a good future
improvement.

### Suggested solution

Transform the data files into a different format such as parquet. To do so, you will need to take few things in
consideration:

- When running the crawler for the first time, we change the `StorageDescriptor` (`glue_adapter.py`) for the tables to
  use a CSV formatter
- When running the infra to set up glue (`modules/data-workflow/glue-components.tf`), we are creating
  an `aws_glue_classifier` named `custom_csv_classifier` and is set up in `create_crawler` (`glue_adapter.py`) with the
  string `single_column_csv_classifier`. To use parquet we need to change to a parquet classifier
- In `s3_adapter.py` when storing the file, the format is defined in `_construct_partitioned_data_path`. It works using
  the input filename, and since this already has `.csv` on it, it gets stored with it. To change it we just need to
  replace `.csv` to `.{desiredformat}`

### Assumptions

- Parquet will perform better

## CSRF and XSS protection

### Problem

The front end was done during the last few days, and there are lots of security improvements that could be added, for
example CSRF and XSS protection.

### Suggested Solution

- Enable XSS protection in
  WAF [OWASP XSS Cheatsheet](https://cheatsheetseries.owasp.org/cheatsheets/Cross_Site_Scripting_Prevention_Cheat_Sheet.html)
- Enable CSP headers and restrict inline styling and inline
  scripting [OWASP CSP Cheatsheet](https://cheatsheetseries.owasp.org/cheatsheets/Content_Security_Policy_Cheat_Sheet.html)
- Use the CSRF header for security checks (the CSRF header is enabled by
  default) [OWASP CSRF Cheatsheet](https://cheatsheetseries.owasp.org/cheatsheets/Cross-Site_Request_Forgery_Prevention_Cheat_Sheet.html)

## API Gateway Integration

### Problem

All redirections and security checks are being done inside the application. Also, we do not have a way to easily
implement a "central hub" that talks to several rAPId instances.

### Suggested solution

Using an API Gateway system such as [Kong](https://konghq.com/partners/aws/) to handle redirections and security checks
in Cognito.

### Considerations

#### AWS API Gateway

We tried to implement the AWS API Gateway to solve this issue since it was easy to set up. However, it has a lot of
limitations such as the max payload (10Mb)
. [AWS API Gateway Limits](https://docs.aws.amazon.com/apigateway/latest/developerguide/limits.html)

#### Kong Gateway

We suggest Kong since it has a free version, brings a lot of customization and can be implemented in AWS without too
much trouble.

## Trace requests

### Problem

Currently, there is not enough visibility for the application requests in the logs. We can find any error, but it would
be better if we could trace the steps that had been made before it, and gather information on whoever made a request.

Implementing a tracing functionality will reduce the risk of having any repudiation issues in the application.

### Suggested Solution

There several ways to do this, however, we recommend following one of these:

1) Use a request id generated by the API Gateway (If one is implemented), this would give visibility on every level, and
   would allow the people supporting the app to know if a request has reached the server or if it got any communication
   issues before, then it can be logged at the start of each step to trace visibility on what is happening. The API
   Gateway might have the functionality to log information such as the IP address, therefore, we should be able to track
   exactly who made a particular request at any given time
2) Generate a UUID at the start of every request. Again, we can log it at the start of every step is made in the
   application and trace the whole journey for any given request. There could also be a log that links any request to
   whoever is making it

## One Infrastructure Configuration per Block

### Problem

When instantiating the infrastructure for a department, specific configuration should be provided to determine what is
the rapid service domain, which is the S3 bucket to store date, etc.

Currently, this is done by one configuration file located at the repository `rapid-infrastructure-config` or another
configuration file informed by the user. Therefore, one file concentrates all the config parameters which are later sent
to each infra terraform block.

Due to that each block receives more parameters than it needs since one file contains all of them.

### Suggested solution

In order to solve this problem, one approach is to have multiple config files focused on the respective terraform block
plus a common terraform config file.

Instead of having one file called `input-params.tfvars`, this can be broken down as:

* `common-params.tfvars` => contains parameters that are common by 2 or more blocks
* `auth-params.tfvars` => specific parameters for authentication block with cognito
* `app-cluster.tfvars` => specific parameters for app-cluster module
* etc.

For each block, the terraform command invocation also needs to change, so it can call 2 configuration files. See example
below:

`terraform apply /location/app-cluster/main.tf -var-file=/location/common-params.tfvars -var-file=/location/app-cluster.tfvars`
