---
id: backend-installation
title: "Backend Installation"
hide_title: true
hide_table_of_contents: true
sidebar_position: 2
---

# Tuva EMPI Backend

The backend for Tuva EMPI. It consists of a Django API and PostgreSQL database.

## Development

### Installation

Inside the dev Docker container, you can run:

1. `cd backend`
1. `make install-all`
1. `make migrate`
1. `make bootstrap`
1. `make run-dev`

### Testing and formatting

1. Run type checking: `make check`
1. Run type checking and tests: `make test`
1. Run formatter: `make format`
1. Run linter: `make lint`

#### Testing with localstack S3

When running locally, if you'd like to use the /person-records/import API endpoint in order to test things, you can use `localstack` via the `awslocal` command.

##### Example

In a dev container terminal:

- Run the API dev server: `make run-dev`

Then, in another dev container terminal `cd backend` and run:

- Create a bucket: `aws s3api create-bucket --bucket tuva-health-local`
- Upload a person records file: `aws s3 cp main/tests/resources/tuva_synth/tuva_synth_clean.csv s3://tuva-health-local/raw-person-records.csv`
- Open a web browser on the host and visit `localhost:9000`
- Sign-in (there is an initial test user created with username `user` and password `test1234`)
- Once signed in you might see a 502 bad gateway if the frontend web server isn't running. That's okay, we just want to open the dev tools, go to cookies and copy the OAuth2 Proxy cookies (e.g. `_oauth2_proxy`) into environment variables in the dev container: `export AUTH_COOKIE="..."`. It's possible that there is more than one cookie.
- POST to the config endpoint with cookie: `http -v oauth2-proxy:9000/api/v1/config splink_settings:=@main/tests/resources/tuva_synth/tuva_synth_model.json potential_match_threshold:=0.5 auto_match_threshold:=1 "Cookie:_oauth2_proxy=$AUTH_COOKIE"`
- POST to the import endpoint: `http -v oauth2-proxy:9000/api/v1/person-records/import s3_uri=s3://tuva-health-local/raw-person-records.csv config_id=cfg_1 "Cookie:_oauth2_proxy=$AUTH_COOKIE"`

#### Processing jobs

- Running the `matching service` so it can process jobs: `make worker`

#### Connecting to the DB directly

When running locally, you can connect to the PGSQL database from a dev container terminal via: `PGPASSWORD=tuva_empi psql -h db -U tuva_empi`

#### Clearing the database

If you'd like to start from scratch with Postgres, either delete the Docker volume where the PGSQL data is stored or drop and recreate the database:

1. `PGPASSWORD=tuva_empi psql -h db -U tuva_empi postgres`
1. `drop database tuva_empi`
1. `create database tuva_empi`

#### Testing with AWS Cognito

If you'd like to connect to AWS Cognito, you will need to create an AWS account and setup a test AWS Cognito user pool. For the callback URL in cognito, use `http://localhost:9000/oauth2/callback`.

If you are switching from Keycloak, it's best to remove all containers (`cd .devcontainer && docker compose down`) and clear your database by removing the DB volume.

Then locally, you will need to disable localstack and load your AWS config.

In `.env`:

1. Uncomment `AWS_CONFIG_FILE`
1. Comment out `AWS_ENDPOINT_URL`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`
1. Modify `OAUTH2_PROXY_OIDC_ISSUER_URL`, `OAUTH2_PROXY_SCOPE`, `OAUTH2_PROXY_CLIENT_ID` and `OAUTH2_PROXY_CLIENT_SECRET` to match your AWS Cognito test user pool settings (AWS Cognito doesn't support `offline_access` scope)

In `local.json`:

1. Set `idp.backend` to `aws-cognito`
1. Set `cognito_user_pool_id`, `jwks_url` and `client_id` based on your AWS Cognito test user pool settings
1. Set `initial_setup.admin_email` to a valid user's email address from your test user pool

Then you can rebuild and reopen the backend docker container.

In the dev container:

1. Set AWS profile: `export AWS_PROFILE=...`
1. Login to AWS: `aws sso login`
1. Check your identity: `aws sts get-caller-identity`
1. Then follow the steps in `Installation` to run migrations and bootstrap

#### Testing with kind

If you'd like to test the K8sJobRunner, you can use kind. kind is included in the backend dev container. kind works by running k8s in Docker using your host's Docker instance (the Docker socket is mounted to the backend dev container).

First make sure you are in the `backend` directory:

1. `cd backend`

Since kind uses the host's Docker instance, you only need to create a cluster once even after rebuilding the backend dev container:

1. Create the cluster: `kind create cluster --name dev`
1. Attach the kind cluster to our Docker Compose network: `docker network connect tuva-empi_app-network dev-control-plane`
1. Create a k8s secret for the backend config file: `kubectl create secret generic tuva-empi-backend-dev-config --from-file=deployment.json=config/local.json`

However, each time you rebuild the backend container, you need to update the kubeconfig:

1. Add the kubeconfig from kind to our backend container: `mkdir -p ~/.kube && kind get kubeconfig --name dev > ~/.kube/config`
1. Retrieve the cluster's CA public key: `mkdir -p ~/.kube/certs && docker exec dev-control-plane cat /etc/kubernetes/pki/ca.crt > ~/.kube/certs/kind-ca.crt`
1. Update the k8s server so that we can access it from our backend container: `kubectl config set-cluster kind-dev --server=https://dev-control-plane:6443 --certificate-authority="$HOME/.kube/certs/kind-ca.crt"`

Then you can build and load the backend image:

1. Build the production image: `docker build -t tuva-empi-backend .`
1. Load the production image: `kind load docker-image tuva-empi-backend:latest --name dev`

Then you can test the K8sJobRunner by configuring the MatchingService to use it and starting the MatchingService in the backend dev container as usual.

### Migrations

To re-apply migrations from scratch:

1. Clear the database: `python manage.py flush`
1. Undo migrations: `python manage.py migrate main zero`
1. Apply migrations: `make migrate app=main`

To add a new auto migration:

1. Make changes to the models
1. `python manage.py makemigrations`

To add a new manual migration:

1. Create a new migration file in the migrations directory

To run migrations:

1. `make migrate app=main`

### Config

Currently, there are a couple config environments:

- local (local development)
- ci (Github Actions)

But really, an environment is just a name. And the only one that has any special meaning is "local", because settings.py sets certain things if the env is set to "local".

## Contributing

### Django ORM

In general, prefer raw SQL to the Django ORM for complex queries and queries involving a join. For complex queries, it's more clear to stick with a single mental model (SQL) and it's often more performant. We can craft the query exactly how we want it to be and easily review that it is how we want it to be. For queries involving joins, the Django ORM doesn't provide the most intuitive interface. For example, `select_related` switches between LEFT OUTER and INNER join based on different conditions: https://github.com/django/django/blob/56e23b2319cc29e6f8518f8f21f95a530dddb930/django/db/models/sql/query.py#L1121-L1133. This seems like an easy way to introduce a critical bug. Data integrity is extremely important in this application, so let's not muddy the waters with complex abstractions. For simple create/get/filter/count/update queries, I think the Django ORM is fine and can be easier to read.
