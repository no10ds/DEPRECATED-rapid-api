# Contributing

## Familiarisation 🧊

Familiarise yourself with the following documentation:

1. [Developer usage guide](dev-usage.md)
2. [Application context](application_context.md)
3. [ADRs](../../architecture/adr/)
4. [Infrastructure guides](https://github.com/no10ds/rapid-infrastructure)
5. [Application limitations](application_limitations.md)
5. [Future improvements](improvements.md)

## Prerequisites ✅

### Access

- Git repo
- AWS account

### Tools

Some of these can be installed using the 'Self Service' app that comes with the government laptop. Others you can
download from the linked sites or use Homebrew etc.

(If you install the XCode CLI tools, you'll get Git and Make for free)

- jq (use Homebrew)
- Git
- [Make](https://formulae.brew.sh/formula/make)
- [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)
- [Homebrew](https://brew.sh/)
- Docker (from Self Service)
- [Java version 8+](https://mkyong.com/java/how-to-install-java-on-mac-osx/) (for Batect (future releases will remove
  this dependency))
- Python (v3.10) - Only for local development without Docker (ideally manage
  via [pyenv](https://github.com/pyenv/pyenv))

## Local set up 🏗

We use [batect](https://batect.dev/docs/) to ensure consistency of build and runtime environments.

A [Makefile](../../../Makefile) is provided for convenience. The commands serve as a wrapper around Batect which runs
the various commands in a clean Docker container each time. The commands should be self-explanatory.

Provided you have the above prerequisites you can get started straight out of the box:

1. In the root of the project run `make precommit`. This will run the `precommit` task and, in doing so, create the
   relevant Docker image with relevant dependencies with which to run subsequent tasks and the app locally.
2. If your IDE can use a remote Docker interpreter, point to the image created above (avoids the need to install a
   specific python version locally or to have to manage virtual environments manually)
    1. Point your IDE to the Python interpreter -> Setup steps are dependent on IDE of choice
3. If your IDE CANNOT use a remote Docker interpreter, use a local `venv` (see setup instructions below) and point to
   this Python interpreter
    1. Setup steps are dependent on IDE of choice

4. Add AWS credentials for the CLI.
   Follow [these instructions](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html)

## Running locally 🏃‍♂️

To run the app locally, you will need to set the following environment variables:
- `AWS_ACCOUNT` 
- `AWS_REGION` 
- `AWS_DEFAULT_REGION` 
- `COGNITO_USER_POOL_ID` 
- `DATA_BUCKET` 
- `DOMAIN_NAME` 
- `RESOURCE_PREFIX`

`make run` runs batect to bring up a locally running version of the application within a Docker container using the base
image.

`make run-dev` runs batect to bring up a locally running version of the application within a Docker container in a "
hot-reload" mode so you can keep this running during development.

> ⚠️ Note that the app communicates with AWS services and needs to assume the relevant role to do so, however the Docker container **will not have this role**.
>
> To run the app locally with an admin context you can assume the admin or user role by running `make assume-role` in the `rapid-infrastructure` repo and running the application directly using the `uvicorn` command that batect runs
>
> Of course, this requires that you have Python installed on your machine (see above)

## Developing 🤓

### Managing the environment

`Make` tasks that require the environment to be defined and set-up, will ensure that the latest image has been built and
is used for running the task.

If you would like to manually recreate the environment (e.g.: after specifying a new dependency etc.) you can
run `make create-runtime-env`. This will also output the name of the created Docker image for your convenience.

To create a local virtual environment and install dependencies run `make create-local-venv`. This is only used for local
development and serves as a workaround in case your IDE of choice cannot use a remote Docker python interpreter. Ensure
you remember to activate the `venv` once it has been created.

To clean your workspace run `make clean-env`. Currently, this removes the virtual environment, environment image, pytest
caches and coverage reports.

### Dependency management

Batect will build the Docker image with all relevant dependecies baked in. No input on your part will be required.

To install all dependencies, if you are using a local `venv` simply activate the virtual environment and
run `pip install -r requirements.txt`.

To add a new dependency, if you are using a local `venv` simply activate the virtual environment and
run `pip install <dependency>` and freeze this into the requirements file: `pip freeze > requirements.txt`

### General maintenance

You can run `make shell` to run commands in a clean environment against the mounted local source code; e.g.: Install
dependencies, etc

### Testing

To run the tests in your IDE ensure that the test runner's working directory is set to the root directory of the
project.

For PyCharm, go to `Run Configurations > Edit Configurations > Edit Configuration Templates > Python Tests`
Then, edit the `Working Directory` field of BOTH the `Autodetect` and `pytest` configuration templates to be the
absolute path to the root project directory e.g.: `/Users/user/Documents/rapid-api`

You will need to add some environment variables to your run configuration templates too (with sample values below):

- `DATA_BUCKET=the-bucket`
- `AWS_ACCOUNT=123456`
- `AWS_REGION=eu-west-2`
- `RESOURCE_PREFIX=rapid`
- `DOMAIN_NAME=example.com`
- `COGNITO_USER_POOL_ID=11111111`


### Checking your code

`make precommit` will lint source code, check for secrets and security vulnerabilities, validate config and run tests

### Security

#### Vulnerabilities

To scan for security vulnerabilities run `make security`

We use [bandit](https://pypi.org/project/bandit/) to check for common python vulnerabilities

#### Scanned image vulnerabilities

During deployment, once the image has been pushed to ECR, we automatically scan it for vulnerabilities. The pipeline has
a stage after upload to check the results of the scan and fail if some are found (currently only with high and critical
severity).

AWS also automatically scans the images every day to check for any new vulnerabilities. We have a scheduled pipeline
that checks the results of this scan and fails if the image becomes insecure.

Once you have reviewed the vulnerability, and you deem it acceptable, you can ignore it by adding the `CVE` to a new
line in the `vulnerability-ignore-list.txt` file. Ensure you commit with a meaningful error message, the URL and your
reasoning for ignoring it. By doing this the pipeline will accept that vulnerability and _not_ fail the build.

#### Secrets

We are using the python package [detect-secrets](https://github.com/Yelp/detect-secrets) to analyse all files tracked by
git for anything that looks like a secret that might be at risk of being committed.

There is a `.secrets.baseline` file that defines any secrets (and their location) that were found in the repo, which are
not deemed a risk and ignored (e.g.: false positives)

To check the repo for secrets during development run `make detect-secrets`. This compares any detected secrets with the
baseline files and throws an error if a new one is found.

> ⚠️ Firstly, REMOVE ANY SECRETS!

However, if the scan incorrectly detects a secret, run: `make ignore-secrets` to update the baseline file. **This file
should be added and committed**.

The secret detection step is run as part of the `make precommit` target.

#### Formatting

To format the code or check for linting errors we use `black` and `flake8`. Use the make targets for convenience:

`make format`

`make lint`

### Testing your code

We use pytest as our test runner.

`make test`

### Committing

We follow a consistent commit message structure:

``` text
<issue_number>-<initials> Short message

Message body
```

For example:

``` text
455-USR Fix codestyle config

Removing extra semicolon causing the config validation to fail
```

## Deployment and Infrastructure 🚀

Service infrastructure is stored in a [separate repository](https://github.com/no10ds/rapid-infrastructure).

See README and docs in that repository for more details

### Pipeline

We are using Github Actions as the pipeline solution, with a self-hosted runner hosted in AWS.

The config for the pipeline is in `.github/workflows/main.yaml`

#### Image tagging

As the code moves through the pipeline and passes or fails successive checks, the built image is tagged accordingly.

1. Initially all images are tagged with their `<commit-hash>`
2. If the image vulnerability scan check stage finds issues, the image is tagged with `VULNERABLE-<commit-hash>` and the
   deployment aborts
3. If no vulnerabilities are found, a subsequent pipeline stage tags the image as `PROD`
4. During deployment, the ECS Task Definition looks for the latest image tagged with `PROD` and deploys it
5. Subsequent taggings of new deployment candidates simply override the previous `PROD` tag

## Releasing

The guide for how to perform a release of the service image.

### Context

The product of the rAPId team is the service image that departments can pull and run in their own infrastructure.

Performing a release fundamentally involves tagging the image of a version of the service with a specific version number
so that departments can reference the version they want when pulling from ECR.

### Prerequisites

- Download the GitHub CLI with `brew install gh`
- Then run `gh auth login` and follow steps

### Steps

1. Decide on the new version number following the [semantic versioning approach](https://semver.org/)
2. Update and commit the [Changelog](../../../changelog.md) (you can follow
   the [template](../../../changelog_release_template.md))
3. Run `make release commit=<commit_hash> version=v.X.X.X`

> ⚠️ Ensure the version number follows the format `v.X.X.X` with full-stops in the same places, particularly the first one after the `v`

Now the release pipeline will run automatically, build the image off that version of the code, tag it and push it to ECR

### How to add security to an endpoint

We are using the FastApi Security package. Security takes dependencies and scopes as arguments. In our case the
dependency are the ```protect_endpoint```, ```protect_dataset_endpoint``` methods, and one or more of the following
action scopes:

- `READ`
- `WRITE`
- `DATA_ADMIN`
- `USER_ADMIN`

For instance, if `WRITE` scope is used, that means that whoever is trying to access the endpoint needs to have any
of `WRITE_ALL`, `WRITE_<sensitivity_level>`, `WRITE/<domain>/<dataset>` scopes listed in their permissions or be part of
that user group, where sensitivity level is the sensitivity level of the dataset being modified. Otherwise, the requests
fails.

> ⚠️ ️NOTE: Higher sensitivity levels imply lower sensitivity levels.

To add security to the endpoint, add specify the `dependencies=` keyword argument and specify the scopes in the endpoint
annotation as listed in the examples below:

* For endpoints **without** ```domain``` and ```dataset``` in the url path, use the dependency ```protected_endpoint```:

```
@app.post("/schema", dependencies=[Security(protect_endpoint, scopes=[Action.READ.value])])
``` 

or

* For endpoints **with** ```domain``` and ```dataset``` as part of the url path, use the
  dependency ```protected_dataset_endpoint```:

```
@app.get("/{domain}/{dataset}/info",
                     dependencies=[Security(protect_dataset_endpoint, scopes=[Action.READ.value])])
``` 

### How to add security to the endpoint - Front End Layer

Some pages in the frontend layer are only accessible after the user has authenticated, e.g.: the `/upload` page. In
order to protect them, the dependency functions ```protect_endpoint``` and ```protect_dataset_endpoint``` should be
included in the Fast API endpoint annotation as described in the example below:

```
@app.get("/upload", include_in_schema=False,
         dependencies=[Depends(protect_dataset_endpoint)])
```

Note that ```protect_dataset_endpoint``` dependency function must be used when ```domain``` and ```dataset``` from is
present in the url path and should be taken in consideration to determine the permission to access.

When using the frontend layer instead of the client app token, the user token is used. This token contains Cognito user
groups to describe the permission access level for that particular user. The cognito follows a naming convention
of ```WRITE/<domain>/<dataset>``` such as ```WRITE/trains/completed_journeys```.

Note: The system expects the user group naming to follow the convention above otherwise it will fail to determine the
permission level.

## Gotchas 🤯

- When using `Pyenv` you will probably be using v2+. If this is the case, ensure you add `$(pyenv root)/shims` to your
  PATH and NOT `$(pyenv root)/bin`.
- If you find yourself needing to delete a Data Catalog table, and it is failing silently, ensure you have set an Output
  Location for the results (an S3 bucket).
- If you need to delete a tag run `git tag -d <tag_name>` to delete the tag locally. Then
  run `git push --delete origin <tag_name>` to delete the remote one.
- The release pipeline will only run if the tagged commit contains the relevant workflow config file. That is, you
  cannot commit a workflow config file now and run a previous version of the codebase that does not have that file
  through the pipeline.
- If you need to return csv content from the API, you need to use Response object (PlainTextResponse specifically)
  instead of returning a string.
- When logging out the "Rapid Access Token" will be deleted from the session, however, the cognito session remains and
  clicking in "login" will authenticate the same user automatically.