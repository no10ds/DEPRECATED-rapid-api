# E2E Tests

## Initial Setup

The E2E tests require some first time setup. The tests will hit the real endpoints and some validation checks are
performed that require the relevant resources to be available in AWS (files in S3, Crawlers, Glue Tables, etc.).

### Data setup
Using the running service:

1. Upload the new schemas in `test_files/schemas` to their respective domains (e.g.: `test_e2e/upload`, `test_e2e/query`
   , `test_e2e_protected/do_not_delete`, etc.)
2. Upload the `test-journey_file.csv` to each of the datasets
3. Create a protected domain for `test_e2e_protected`

### Clients setup
Using the running service:

1. Create four programmatic clients with relevant permission (see table below)
2. Add the clients' credentials as secrets (with the name specific in the table below) in AWS Secrets Manager with the structure:
```json
{"CLIENT_ID":  "<client_id>", "CLIENT_SECRET":  "<client_secret>"}
```
| Secret name                        | Client Permissions     |
|------------------------------------|------------------------|
| E2E_TEST_CLIENT_WRITE_ALL          | WRITE_ALL              |
| E2E_TEST_CLIENT_DATA_ADMIN         | DATA_ADMIN, USER_ADMIN |
| E2E_TEST_CLIENT_USER_ADMIN         | USER_ADMIN             |
| E2E_TEST_CLIENT_READ_ALL_WRITE_ALL | READ_ALL, WRITE_ALL    |


## Running the tests

The tests are run in the deployment pipeline.

To run them locally against the live instance, use:

```bash
make test-e2e
```

## Gotchas

If some tests return an HTTP status of 429, this is due to crawlers still running (or in their stopping phase). This
generally happens when the E2E tests are run multiple times in quick succession. Wait for the crawlers to finish and
re-run the tests.
