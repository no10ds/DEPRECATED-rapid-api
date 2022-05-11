# Project rAPId
![Deployment Pipeline](https://github.com/no10ds/rapid-api/actions/workflows/main.yml/badge.svg)

⚠️ This is an MVP, feedback is welcome!

# Product Vision 🔭


Project rAPId aims to create consistent, secure, interoperable data storage and sharing interfaces (APIs) that enable
departments to discover, manage and share data and metadata amongst themselves.

This will rapidly improve the government's use of data by making it more scalable, secure, and resilient, helping to
match the rising demand for good-quality evidence in the design, delivery, and evaluation of public policy.

The project aims to deliver a replicable template for simple data storage infrastructure in AWS and a RESTful API to
ingest and share named, standardised datasets.

<br />
<p align="center">
<a href="https://ukgovernmentdigital.slack.com/archives/C03E5GV2LQM"><img src="https://user-images.githubusercontent.com/609349/63558739-f60a7e00-c502-11e9-8434-c8a95b03ce62.png" width=160px; /></a>
</p>

# Deploying a rAPId Instance

Please reach out to us on [slack](https://ukgovernmentdigital.slack.com/archives/C03E5GV2LQM) if you would like the rAPId team to deploy and manage a rAPId instance on your behalf.

Or you can consult the [Infrastructure Repo](https://github.com/Project-rAPId/rapid-infrastructure) for guidance on how to spin up an instance yourself.

# Using the rAPId service 🙋

Please see the [usage guide](docs/guides/usage/usage.md)

# Contributing 🤓

Please see the [Contributing README](docs/guides/contributing/contributing.md)

# High Level Architecture 🏡

Diagrams are compiled [here](docs/architecture/C4_diagrams) and show the intended architecture solution

# Tech Stack 🍭

- Python
- FastApi
- Docker
- AWS (ECR, ECS, EC2, S3, Athena)
- Terraform
- Github Actions
