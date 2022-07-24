# Application Limitations

This document outlines some of the limitations that currently restrict parts of the application and its usage.

## File size limitations

When uploading a file, it is first uploaded to the application instance, available in memory and validated on the fly.
This means that current memory resource limits on the cluster instance prohibit large files from being uploaded.

Files larger than about 500mb cause timeouts and resource exhaustion.

Potential remedies:
- Increase memory resource allocation on the cluster instance
- Upload the file directly to S3, then load it into memory in batches to validate it


## Performance limitations

There are few areas that are bottlenecks
- Double upload
  - During the upload flow two uploads are performed:
    1. To the application instance
    2. Then to S3
  - This should be optimised
- No caching
  - Could look at caching query results (beyond what comes out of the box with Athena)
  - Caching responses for other endpoints if no changes have occurred in the meantime


## Security

### Logging & Request Tracing

We currently do not have widespread logging coverage.

Logging is not occurring for at least:
- Logins
- Permission denied operations
- Who performed which operations

Additionally, support for passing a request ID through the different levels of the application stack exists, but has not been fully completed.


### Querying big files in the docs (Open API issue)

If you try to query a big file from the `/docs` endpoint, the page will freeze, even though a service returns 200 status
code. That's due to OpenAPI issues.


### Permissions
- In `authorisation_service.py:match_user_permissions` we are currently not checking the user token groups for endpoints that don't contain domain and dataset
  - This makes sense for now since the only action a user can do is `WRITE` to the `/upload` endpoint via the custom UI
  - In the future, when more endpoints will be accessible by the user, this method should be extended to check the actions a user is allowed to perform the requisite operation
- We are not handling anything other than the `WRITE` action on specific domain/dataset
  - Again, this is reasonable for the time being since this is the only action that a UI user can perform
- Currently, we are only dynamically creating the `WRITE` group during schema upload.
  - If `READ` functionality added, `READ` groups should be created here too, and handled accordingly throughout the authorisation flow

### SQL Injection
- We are currently not checking for SQL injection in the code beyond what the firewall (WAF) rules give us.
  - The WAF has an allocated time budget to check the request so if the request is large or the processing is slow for whatever reason there is a risk that malicious code could be introduced.
