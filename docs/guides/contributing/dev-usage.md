# Developer Usage Guide

This usage guide is aimed at developers maintaining the rAPId service.

## Overview of application

The core concept of the application is the upload and querying of data. The intention is that different departments will
run their own instance of the application.

The data that users upload is organised by `domain` and `dataset`.

`Domain` - Some higher level categorisation of multiple datasets, e.g.: `road_transport`, `border`, etc.
`Dataset` - The individual dataset name as it exists within the domain, e.g.: `electric_vehicle_journeys_london`
, `confirmed_visa_entries_dover`, etc.

### Usage flow

This section lays out what happens at different stages when using the application. Although high-level, this should help
to clarify the overarching structure and where to look when things go wrong.

1. Application service spins-up
    1. No domains or datasets exist
    2. No user groups exist
2. Client app registered and given desired scopes
3. Client app uploads schema to define the first dataset
    1. _User_ group created in Cognito, in anticipation of granting users access to upload data to the dataset
4. User registered and assigned to the desired user groups
5. User logs in to the UI
6. User uploads dataset file
7. AWS Glue Crawler runs to look at the data a construct a metadata schema in the Glue Catalog
8. The data is available to be queried by a client app via the `/docs` page or via a programmatic client

## Authorisation flows

Two main flows are currently supported:

- Client app
- User

The client app is a programmatic client that is intended as the main way to interact with the API. Client apps are
currently able to log into and use the `/docs` page and to use the API by programmatic means (custom app, script, cURL,
Postman, etc.)

The user is a human element that uses the custom UI. Currently, the UI only supports file upload but the intention is to
extend this in the future.

| Flow       | Token        | Auth method | Permission example                                 | Notes                                                             |
|------------|--------------|-------------|----------------------------------------------------|-------------------------------------------------------------------|
| User       | User Token   | User groups | `WRITE/domain1/dataset1`, `WRITE/domain2/dataset1` | No specificity at the sensitivity level, only domain and dataset  |
| Client app | Client Token | Scopes      | `WRITE_PUBLIC`, `READ_SENSITIVE`                   | No specificity at the domain or dataset level, only sensitivity   |

The "action" component of a permission (`READ`, `WRITE`, etc.) is used only in the matching logic when a request is made
and compared to the specified scope assigned to the endpoint being accessed.
