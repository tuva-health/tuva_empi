---
id: production-environment
title: "Production Environment"
hide_title: true
hide_table_of_contents: true
sidebar_position: 2
---

# Production Environment

The following are notes and guidelines around deploying Tuva EMPI in production. Tuva EMPI is a containerized web application primarily designed to be run on [Kubernetes](https://kubernetes.io/).

## OCI Images

We produce two OCI images, one for the frontend and one for the backend:

### Frontend

Repo: https://github.com/tuva-health/tuva_empi/pkgs/container/tuva-empi-frontend

The frontend image runs the frontend NextJS application, listening on port 3000. It doesn't support any parameters at the moment.

### Backend

Repo: https://github.com/tuva-health/tuva_empi/pkgs/container/tuva-empi-backend

The backend image contains the entire Tuva EMPI backend which includes the API, matching service, migrations and bootstrapping. The API listens on port 8000.

It accepts the following parameters:

- `TUVA_EMPI_CONFIG_FILE`: Path to Tuva EMPI backend config file (the actual config file can be mounted as a volume)
- `TUVA_EMPI_CONFIG_AWS_SECRET_ARN`: ARN of AWS Secrets Manager secret containing the config file (`TUVA_EMPI_CONFIG_FILE` takes priority)

For full details on how to configure the Tuva EMPI backend, see: [Configuration](../configuration)

## Deploying Containers

Given the images above, you can deploy the following containers which make up Tuva EMPI:

- frontend (image: `tuva-empi-frontend`, command: none)
- api (image: `tuva-empi-backend`, command: none)
- matching-service (image: `tuva-empi-backend`, command: `matching-service`)

Before deploying containers for the backend, you should run migrations and bootstrapping.

### Migrations

Migrations should be run on each new version of Tuva EMPI before deploying new containers for that version.

Migrations can be run using the `tuva-empi-backend` image with the `migrate` command. The container should run and then exit.

:::caution
Only one instance of `migrate` should be run at a time.
:::

### Bootstrapping

When starting Tuva EMPI for the first time, you need to bootstrap it in order to register an initial admin user. The configured initial admin user should already exist in your external identity provider.

Bootstrapping can be run using the `tuva-empi-backend` image with the `bootstrap` command. The container should run and then exit.

:::info
Bootstrapping only needs to be done once.
:::

### API

Once migrations and bootstrapping have been run, you can deploy the API container.

### Matching Service

Once the API container has been deployed, you can deploy the Matching Service container.

:::caution
You should fully deploy a new version of the API container before deploying the corresponding version of the matching service container.
:::

### Frontend

Once the API container has been deployed, you can deploy the frontend container.

:::caution
You should fully deploy a new version of the API container before deploying the corresponding version of the frontend container so that the frontend doesn't depend on changes that do not exist in the API container.
:::

## Identity Provider

Tuva EMPI relies on an external identity provider for it's authentication needs. Before deploying Tuva EMPI you should setup your external identity provider. See [Architecture](../architecture/overview) for more information on supported identity providers.

:::info
It's recommended that the identity provider used for Tuva EMPI is only used for Tuva applications and not shared with other applications in your environment. That's because we may depend on features of the identity provider (e.g. JWT claims) in an application-specific way.
:::

:::caution
Both AWS Cognito and Keycloak must be configured to use email as the username. We use email to lookup users in the identity provider.
:::

## Database

Tuva EMPI relies on a PostgreSQL (16 or higher) database as it's primary data store.

:::info
It is recommended to create the Tuva EMPI database on an isolated database server used only by Tuva applications.
:::

## AWS Secrets Manager

When deploying to AWS, you can take advantaged of AWS Secrets Manager for storing your Tuva EMPI configuration JSON. See the `TUVA_EMPI_CONFIG_AWS_SECRET_ARN` variable above.

## Example AWS Deployment Architecture

Here is one possible example of how you can deploy Tuva EMPI in AWS:

![Tuva EMPI Example AWS Deployment Architecture SVG](/img/empi-aws-example-architecture.svg)
