# rAPId API Usage Guide

The rAPId API serves to make data storage and retrieval consistent for all users involved.

Overarching API functionality includes:

- Uploading a schema
- Uploading data that matches that schema
- Listing available data
- Querying data
- Deleting data
- Creating programmatic clients

> ⚠️ Currently the **custom UI** only supports **uploading datasets**

## Application usage overview

The first step is to create a dataset by uploading a schema that describes the metadata including e.g.: data owner, tags, partition columns, data types, etc.. 

Then the data (currently only `.csv` files are supported) can be uploaded to the dataset. During the upload process, the service checks if the data 
matches the previously uploaded dataset schema definition.

During upload, a data 'crawler' is started which looks at the persisted data and infers some metadata about it. Once the crawler
has finished running (usually around 4-5 minutes) the data can be queried.

The application can be used by both human and programmatic clients (see more below)
- When accessing the REST API as a client application, different actions require the client to have different permissions e.g.:`READ`, `WRITE`, `ADD_SCHEMA`, etc., and different dataset sensitivity level permissions e.g.: `PUBLIC`, `PRIVATE`, etc.
- When accessing the UI as a human user, permissions are granted by user groups e.g.: `WRITE/trains/completed_journeys`

## Data upload and query flows

### No schema exists + upload data + query

![general usage flow image](diagrams/general_usage_flow.png)

### Schema exists + upload data + query

![upload and query image](diagrams/upload_and_query_data.png)

# How to authorise

## Human Users

### Granting users permissions
If human users are involved in the process then they should be given permission to each dataset they need to upload data to. 

This step is done via the AWS console in the Cognito section. 

To do this:
1. Navigate to the corresponding Cognito user pool of this application (check your config file for the user pool name)
2. Add the desired users to the dataset user groups. 
   1. Datasets user groups should follow the naming convention ```WRITE/<domain>/<dataset>```. 
   2. e.g.: For the domain ```trains``` and dataset ```completed_journeys``` the user group should be ```WRITE/trains/completed_journeys```. 
   3. Once a user is added to dataset user group as described above, the user will need to re-authenticate on the frontend so that their access token
   reflects the new permissions.

## Programmatic clients

### Granting client apps permissions

When creating a client app via the `/client` endpoint, scopes can be granted.

To update these, currently an admin will need to go to Cognito user pool in the AWS console and manually grant or revoke the relevant scopes to the client app. 

## Authenticating and interacting with the application

### Client app

#### Using the OpenApi docs at `/docs`:

1. Hit Authorise button
2. Pass `client id` and `client secret`
3. Access the endpoints

#### Via programmatic access:

See the [rapid test client](https://github.com/Project-rAPId/rapid-test-client/blob/main/test_client.py) repository for
an example of programmatic client configuration.

The general concept is to retrieve an access token using client credentials and making subsequent requests passing that
token.

### Human user

#### Via the UI

Clicking 'Login' on the `/login` page will redirect the user to Cognito, whereupon they will
be prompted to enter their username and password. This will grant them a temporary access token and redirect them to the `/upload` page.

# Endpoint usage

The following documents the usage of the available endpoints exposed by the REST API. 

## Generate schema

In order to upload the dataset for the first time, you need to define its schema. This endpoint is provided for your
convenience to generate a schema based on an existing dataset. Alternatively you can consult
the [schema writing guide](schema_creation.md) if you would like to create the schema yourself. You can then use the
output of this endpoint in the Schema Upload endpoint.

### General structure

`POST /schema/{sensitivity}/{domain}/{dataset}/generate`

### Inputs

| Parameters    | Usage                                   | Example values               | Definition            |
|---------------|-----------------------------------------|------------------------------|-----------------------|
| `sensitivity` | URL parameter                           | `PUBLIC, PRIVATE, SENSITIVE` | sensitivity of the dataset |
| `domain`      | URL parameter                           | `land`                       | domain of the dataset |
| `dataset`     | URL parameter                           | `train_journeys`             | dataset title         |
| `file`        | File in form data with key value `file` | `train_journeys.csv`         | the dataset file itself |

### Outputs

Schema in json format in the response body:

```json
{
  "metadata": {
    "domain": "land",
    "dataset": "train_journeys",
    "sensitivity": "PUBLIC", 
    "key_value_tags": {},
    "key_only_tags": [],
    "owners": [
      {
        "name": "change_me",
        "email": "change_me@email.com"
      }
    ]
  },
  "columns": [
    {
      "name": "date",
      "partition_index": 0,
      "data_type": "date",
      "format": "%d/%m/%Y",
      "allow_null": false
    },
    {
      "name": "num_journeys",
      "partition_index": null,
      "data_type": "Int64",
      "allow_null": false
    }
  ]
}
```

### Accepted scopes

In order to use this endpoint you don't need any scope.

### Examples

#### Example 1:

- Request url: `/schema/PRIVATE/land/train_journeys/generate`
- Form data: `file=train_journeys.csv`

#### Example 2:

- Request url: `/schema/PUBLIC/sea/ferry_crossings/generate`
- Form data: `file=ferry_crossings.csv`

## Upload schema

### General structure

When you have a schema definition you can use this endpoint to upload it. This will allow you to subsequently upload
datasets that match the schema. If you do not yet have a schema definition, you can craft this yourself (see
the [schema writing guide](schema_creation.md)) or use the Schema Generation endpoint (see above).

### General structure

`POST /schema`

### Inputs

| Parameters    | Usage                                   | Example values               | Definition            |
|---------------|-----------------------------------------|------------------------------|-----------------------|
| schema        | JSON request body                       | see below                    | the schema definition |

Example schema JSON body:

```json
{
  "metadata": {
    "domain": "land",
    "dataset": "train_journeys",
    "sensitivity": "PUBLIC",
    "key_value_tags": {"train": "passenger"},
    "key_only_tags": ["land"],
    "owners": [
      {
        "name": "Stanley Shunpike",
        "email": "stan.shunpike@email.com"
      }
    ]
  },
  "columns": [
    {
      "name": "date",
      "partition_index": 0,
      "data_type": "date",
      "format": "%d/%m/%Y",
      "allow_null": false
    },
    {
      "name": "num_journeys",
      "partition_index": null,
      "data_type": "Int64",
      "allow_null": false
    }
  ]
}
```

### Outputs

None

### Accepted scopes

In order to use this endpoint you need the `ADD_SCHEMA` scope.

## Upload dataset

Given a schema has been uploaded you can upload data which matches that schema. Uploading a CSV file via this endpoint
ensures that the data matches the schema and that it is consistent and sanitised. Should any errors be detected during
upload, these are sent back in the response to facilitate you fixing the issues.

### General structure

`POST /datasets/{domain}/{dataset}`

### Inputs

| Parameters    | Usage                                   | Example values               | Definition              |
|---------------|-----------------------------------------|------------------------------|-------------------------|
| `domain`      | URL parameter                           | `air`                        | domain of the dataset   |
| `dataset`     | URL parameter                           | `passengers_by_airport`      | dataset title           |
| `file`        | File in form data with key value `file` | `passengers_by_airport.csv`  | the dataset file itself |

### Output

If successful returns file name with a timestamp included, e.g.:

```json
{
  "uploaded": "2022-01-01T13:00:00-passengers_by_airport.csv"
}
```

### Accepted scopes

In order to use this endpoint you need a relevant `WRITE` scope that matches the dataset sensitivity level,
e.g.: `WRITE_ALL`, `WRITE_PUBLIC`, `WRITE_SENSITIVE`

### Examples

#### Example 1:

- Request url: `/datasets/land/train_journeys/`
- Form data: `file=train_journeys.csv`

#### Example 2:

- Request url: `/datasets/air/passengers_by_airport`
- Form data: `file=passengers_by_airport.csv`

## List datasets

Use this endpoint to retrieve a list of available datasets. You can also filter by the dataset sensitivity level or by
tags specified on the dataset.

If you do not specify any filter values, you will retrieve all available datasets.

### Filtering by tags

#### Filter by the tag key

To filter by tag only use the `"key_only_tags"` property, e.g., `"key_only_tags": ["school_type"]`

This will return all datasets which have this associated tag, regardless of the value of that tag, for example:

- dataset1 -> `school_type=private`
- dataset2 -> `school_type=public`
- dataset3 -> `school_type=home_school`

You can achieve the same by using `"key_value_tags"` property with null as a value: ` "key_value_tags": {"school_type": null}`

#### Filter by the tag key AND value e.g.: `"key_value_tags": {"school_type": "private"}`

This will return all datasets which have this associated tag that _also_ has the specified value, for example:

- dataset1 -> `school_type=private`
- dataset2 -> `school_type=private`
- dataset3 -> `school_type=private`

### Filtering by sensitivity level

#### Filter by specific level e.g. `sensitivity: PUBLIC`

This will return all datasets which have this sensitivity level:

- dataset1 -> `sensitivity=PUBLIC`
- dataset2 -> `sensitivity=PUBLIC`
- dataset3 -> `sensitivity=PUBLIC`

You can also use `"key_value_tags"` to filter by sensitivity level, like this:
`"key_value_tags": {"sensitivity": "PUBLIC"}`

### General structure

`POST /datasets`

### Inputs

| Parameters    | Usage                                   | Example values      | Definition            |
|---------------|-----------------------------------------|---------------------|-----------------------|
| query         | JSON Request Body                       | see below           | the filtering query   |

### Output

Returns a list of datasets matching the query request, e.g.:

```json
[
  {
    "domain": "military",
    "dataset": "purchases",
    "tags": {
      "tag1": "weaponry",
      "sensitivity": "PUBLIC"
    }
  },
  {
    "domain": "military",
    "dataset": "armoury",
    "tags": {
      "tag1": "weaponry",
      "sensitivity": "PRIVATE"
    }
  }
]
```

If no dataset exists or none that matches the query, you will get an empty response, e.g.:

```json
[]
```

### Accepted scopes

You will always be able to list all available datasets, regardless of their sensitivity level, provided you have
a `READ` scope, e.g.: `READ_ALL`, `READ_PUBLIC`, `READ_SENSITIVE`

### Examples

#### Example 1 - No filtering:

- Request url: `/datasets`

#### Example 2 - Filtering by tags:

Here we retrieve all datasets that have a tag with key `tag1` with any value, and `tag2` with value `value2`

- Request url: `/datasets`
- JSON Body:

```json
{
  "key_value_tags": {
    "tag1": null,
    "tag2": "value2"
  }
}
```

#### Example 3 - Filtering by sensitivity:

- Request url: `/datasets`
- JSON Body:

```json
{
  "sensitivity": "PUBLIC"
}
```

#### Example 4 - Filtering by tags and sensitivity:

- Request url: `/datasets`
- JSON Body:

```json
{
  "sensitivity": "PUBLIC",
  "key_value_tags": {
    "tag1": null,
    "tag2": "value2"
  }
}

```

#### Example 4 - Filtering by key value tags and key only tags:

- Request url: `/datasets`
- JSON Body:

```json
{
  "sensitivity": "PUBLIC",
  "key_value_tags": {
    "tag2": "value2"
  },
  "key_only_tags": ["tag1"]
}

```

This example has the same effect as Example 3.

## Schema info

Use this endpoint to retrieve basic information for specific datasets, if there is no data stored for the dataset and
error will be thrown.

When a valid dataset is retrieved the available data will be the schema definition with some extra values such as:

- number of rows
- number of columns
- statistics data for date columns

### General structure

`GET /datasets/{domain}/{dataset}/info`

### Inputs

| Parameters    | Usage                                   | Example values               | Definition            |
|---------------|-----------------------------------------|------------------------------|-----------------------|
| `domain`      | URL parameter                           | `land`                       | domain of the dataset |
| `dataset`     | URL parameter                           | `train_journeys`             | dataset title         |

### Outputs

Schema in json format in the response body:

```json
{
  "metadata": {
    "domain": "dot",
    "dataset": "trains_departures",
    "sensitivity": "PUBLIC",
    "tags": {},
    "owners": [
      {
        "name": "user_name",
        "email": "user@email.email"
      }
    ],
    "number_of_rows": 123,
    "number_of_columns": 2,
    "last_updated": "2022-03-01 11:03:49+00:00"
  },
  "columns": [
    {
      "name": "date",
      "partition_index": 0,
      "data_type": "date",
      "format": "%d/%m/%Y",
      "allow_null": false,
      "statistics": {
        "max": "2021-07-01",
        "min": "2014-01-01"
      }
    },
    {
      "name": "num_journeys",
      "partition_index": null,
      "data_type": "Int64",
      "allow_null": false,
      "statistics": null
    }
  ]
}
```

### Accepted scopes

You will always be able to get info on all available datasets, regardless of their sensitivity level, provided you have
a `READ` scope, e.g.: `READ_ALL`, `READ_PUBLIC`, `READ_SENSITIVE`

### Examples

#### Example 1:

- Request url: `/datasets/land/train_journeys/info`

## List Raw Files

Use this endpoint to retrieve all raw files linked to a specific domain/dataset, if there is no data stored for the
domain/dataset an error will be thrown.

When a valid domain/dataset is retrieved the available raw file uploads will be displayed in list format.

### General structure

`GET /datasets/{domain}/{dataset}/files`

### Inputs

| Parameters    | Usage                                   | Example values               | Definition            |
|---------------|-----------------------------------------|------------------------------|-----------------------|
| `domain`      | URL parameter                           | `land`                       | domain of the dataset |
| `dataset`     | URL parameter                           | `train_journeys`             | dataset title         |

### Outputs

List of raw files in json format in the response body:

```json
[
  "2022-01-21T17:12:31-file1.csv",
  "2022-01-24T11:43:28-file2.csv"
]
```

### Accepted scopes

You will always be able to get info on all available datasets, regardless of their sensitivity level, provided you have
a `READ` scope, e.g.: `READ_ALL`, `READ_PUBLIC`, `READ_SENSITIVE`

### Examples

#### Example 1:

- Request url: `/datasets/land/train_journeys/files`

## Delete Data File

Use this endpoint to delete raw files linked to a specific domain/dataset, if there is no data stored for the
domain/dataset or the file name is invalid an error will be thrown.

When a valid file in the domain/dataset is deleted success message will be displayed

### General structure

`GET /datasets/{domain}/{dataset}/{filename}`

### Inputs

| Parameters | Usage                                   | Example values                  | Definition                    |
|------------|-----------------------------------------|---------------------------------|-------------------------------|
| `domain`   | URL parameter                           | `land`                          | domain of the dataset         |
| `dataset`  | URL parameter                           | `train_journeys`                | dataset title                 |
| `filename` | URL parameter                           | `2022-01-21T17:12:31-file1.csv` | previously uploaded file name |


### Accepted scopes

You will always be able to get info on all available datasets, regardless of their sensitivity level, provided you have
a `DELETE` scope, e.g.: `DELETE_ALL`, `DELETE_PUBLIC`, `DELETE_SENSITIVE`

### Examples

#### Example 1:

- Request url: `/datasets/land/train_journeys/2022-01-21T17:12:31-file1.csv`

## Query dataset

Data can be queried provided data has been uploaded at some point in the past and the 'crawler' has completed its run.

### General structure

`POST /datasets/{domain}/{dataset}/query`

### Inputs

| Parameters    | Required     | Usage                   | Example values             | Definition                    |
|---------------|--------------|-------------------------|----------------------------|-------------------------------|
| `domain`      | True         | URL parameter           | `space`                    | domain of the dataset         |
| `dataset`     | True         | URL parameter           | `rocket_circumnavigations` | dataset title                 |
| `query`       | False        | JSON Request Body       | see below                  | the query object              |

#### How to construct a query object:

There are six values you can customise:

- `select_columns`
    - Which column(s) you want to select
    - List of strings
    - Can contain aggregation functions e.g.: `"avg(col1)"`, `"sum(col2)"`
    - Can contain renaming of columns e.g.: `"col1 AS custom_name"`
- `filter`
    - How to filter the data
    - This is provided as a raw SQL string
    - Omit the `WHERE` keyword
- `group_by_columns`
    - Which columns to group by
    - List of column names as strings
- `aggregation_conditions`
    - What conditions you want to apply to aggregated values
    - This is provided as a raw SQL string
    - Omit the `HAVING` keyword
- `order_by_columns`
    - By which column(s) to order the data
    - List of strings
    - Defaults to ascending (`ASC`) if not provided
- `limit`
    - How many rows to limit the results to
    - String of an integer

For example:

```json
{
  "select_columns": [
    "col1",
    "avg(col2)"
  ],
  "filter": "col2 >= 10",
  "group_by_columns": [
    "col1"
  ],
  "aggregation_conditions": "avg(col2) <= 15",
  "order_by_columns": [
    {
      "column": "col1",
      "direction": "DESC"
    },
    {
      "column": "col2",
      "direction": "ASC"
    }
  ],
  "limit": "30"
}
```

> ⚠️ Note:
>
> If you do not specify a customised query, and only provide the domain and dataset, you will **select the entire dataset**

### Outputs

#### JSON

By default, the result of the query are returned in JSON format where each key represents a row, e.g.:

```json
{
  "0": {
    "column1": "value1",
    "column2": "value2"
  },
  "2": {
    "column1": "value3",
    "column2": "value4"
  },
  "3": {
    "column1": "value5",
    "column2": "value6"
  }
}
```

#### CSV

To get a CSV response, the `Accept` Header has to be set to `text/csv`. The response will come as a table, e.g.:

```csv
"","column1","column2"
0,"value1","value2"
1,"value3","value4"
3,"value5","value6"
```

### Accepted scopes

In order to use this endpoint you need a `READ` scope with appropriate sensitivity level permission,
e.g.: `READ_PRIVATE`.

### Examples

#### Example 1 - Full dataset - JSON:

- Request url: `/datasets/land/train_journeys/query`

#### Example 2 - Full dataset - CSV:

- Request url: `/datasets/land/train_journeys/query`
- Request headers: `"Accept":"text/csv"`

#### Example 3 - Dataset filtered by date and ordered by column:

- Request url: `/datasets/space/rocket_circumnavigations/query`
- Request Body:

```json
{
  "filter": "date >= '2020-01-01' AND date <= '2020-02-01'",
  "order_by_columns": [
    {
      "column": "launch_date"
    }
  ]
}
```

#### Example 4 - Specific columns, aggregation on values after grouping and limiting results:

In this example we get the average rocket payload for rockets in a certain class where that average payload is heavier
than 5000kg.

- Request url: `/datasets/space/rocket_launches/query`
- Request Body:

```json
{
  "select_columns": [
    "rocket_class",
    "avg(payload_weight)"
  ],
  "filter": "rocket_class in ('mini', 'medium', 'large', 'heavyweight')",
  "group_by_columns": [
    "rocket_class"
  ],
  "aggregation_conditions": "avg(payload_weight) > 5000",
  "limit": "10"
}
```

## Create client

As a maintainer of a rAPId instance you may want to allow new clients to interact with the API to upload or query data.

Use this endpoint to add a new client and generate their client credentials.

### General structure

`POST /client`

### Inputs

| Parameters       | Usage               | Example values   | Definition                                                           |
|------------------|---------------------|------------------|----------------------------------------------------------------------|
| `client details` | JSON Request Body   | See below        | The name of the client application to onboard and the granted scopes |

```json
{
  "client_name": "department_for_education",
  "scopes": [
    "READ_ALL",
    "WRITE_PUBLIC"
  ]
}
```

### Client Name

The client name must adhere to the following conditions:

- Alphanumeric
- Start with an alphabetic character
- Can contain any symbol of `. - _ @`
- Must be between 3 and 128 characters

#### Scopes you can grant to the client

Depending on what permission you would like to grant the onboarding client, the relevant scope(s) must be assigned.
Available choices are:

- `READ_ALL` - allow client to read any dataset
- `READ_PUBLIC` - allow client to read any public dataset
- `READ_PRIVATE` - allow client to read any dataset with sensitivity private and below
- `READ_SENSITIVE` - allow client to read any dataset with sensitivity sensitive and below
- `WRITE_ALL` - allow client to write any dataset
- `WRITE_PUBLIC` - allow client to write any public dataset
- `WRITE_PRIVATE` - allow client to write any dataset with sensitivity private and below
- `WRITE_SENSITIVE` - allow client to write any dataset with sensitivity sensitive and below
- `ADD_SCHEMA` - allow client to add a schema for a dataset of any sensitivity
- `ADD_CLIENT` - allow client to add a new client

### Outputs

Once the new client has been created, the following information is returned in the response:

```json
{
  "client_name": "department_for_education",
  "scopes": [
    "READ_ALL",
    "WRITE_PUBLIC"
  ],
  "client_id": "1234567890-abcdefghijk",
  "client_secret": "987654321"
}
```

### Accepted scopes

In order to use this endpoint you need the `ADD_CLIENT` scope

# UI usage

## Login

This page is used to authenticate users.

### General structure

`GET /login`

### Needed credentials

None

### Steps

1) Go to ```/login```
2) Click on "Log in to rAPId"
3) Wait to be redirected to Cognito
4) Type username and password
5) Click on "Log in"
6) Wait to be redirected to ```/upload```


## Logout

This endpoint is used to remove the user's credentials

### General structure

`GET /logout`

### Needed credentials

None

### Steps - On upload page

1) Go to ```/upload``` as an authenticated user
2) Click on "Log out"
3) Wait to be redirected to ```/login```

### Steps - On the browser

1) Go to ```/logout``` as an authenticated user
2) Wait to be redirected to ```/login```

## Upload

This page is used to upload datasets into the rAPId service by authenticated users.

### General structure

`GET /upload`

### Needed credentials

The user must be logged in as a Cognito user with WRITE permissions in order to use this page. The credentials will be read from the
cookie "rat" that stands for "Rapid Access Token".

Note that the page will only show the dataset to which the user has "WRITE" permissions,
regardless of the current status of these datasets.

For example, if the user belongs to the groups "READ/dot/bikes", "WRITE/dot/cars" and "WRITE/dot/trucks" then they will be able to see the datasets "dot/cars" and "dot/trucks".

If the user is missing any permissions, they can be added via Cognito.

### Steps

To upload dataset just follow these simple steps.

1) Log in
2) Go to ```/upload```
3) Select a dataset from the list
4) Choose a valid file for the selected dataset
5) Click on upload button

### Response

When uploading the datasets there are 2 possible responses:
- Success: A message with the uploaded filename will be shown to the user
- Failure: An error message will be shown to the user
