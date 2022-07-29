# E2E Tests

## Initial Setup

The E2E tests require some first time setup. The tests will hit the real endpoints and some validation checks are
performed that require the relevant resources to be available in AWS (files in S3, Crawlers, Glue Tables, etc.).

Using the running service:

1. Upload the new schemas in `test_files/schemas` to their respective domains (e.g.: `test_e2e/upload`, `test_e2e/query`
   , etc.)
2. Upload the `test-journey_file.csv` to each of the three datasets

## Running the tests

The tests are run in the deployment pipeline.

To run them locally against the live instance, use:
```bash
make test-e2e
```
