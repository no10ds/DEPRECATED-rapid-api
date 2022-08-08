# Journey Tests

The tests will hit the real UI pages as though it is a human user.

## Initial Setup

The journey tests require some first time setup:

1. Manually create a user in Cognito that does not have an associated email address
2. Add permissions to the user manually in the database. Add these:

- `READ_ALL`
- `WRITE_ALL`

3. Add the user credentials (username and password) in AWS Secrets Manager in the
   format `{"username": <username>, "password": <password>}` and call the secret `UI_TEST_USER`.

## Environment Variables
- `DOMAIN_NAME` e.g.: `getrapid.link`, `localhost:8000`

## Running the tests

The tests are run in the deployment pipeline.

To run them locally against the live instance, use:

```bash
make test-ui # Headless mode
make test-headed-ui # Headed mode
```
