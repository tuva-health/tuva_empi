# Tuva EMPI Backend

The backend for Tuva EMPI. It consists of a Django API and PostgreSQL database.

## Development

### Installation

#### Prerequisites

1. Install Docker

#### VS Code

1. With VS Code, just open the repository root directory and you should be prompted to open the project in a dev container.
1. Then inside the dev container terminal:
   ```
   > cd backend
   > make install-all
   > make migrate
   > make run-dev
   ```

#### Other IDEs

1. cd `.devcontainer`
1. Build and run the backend app and DB Docker containers: `docker compose up -d`
1. Attach to the backend app container: `docker attach tuva-empi-backend`
1. Then inside the app container:
   ```
   > cd backend
   > make install-all
   > make migrate
   > make run-dev
   ```
1. `Ctrl-p` followed by `Ctrl-q` allows you to exit the container without stopping it
1. To start the app container after stopping it: `docker start -i tuva-empi-app`
1. To start and attach to the app container after restarting your system: `docker compose start` then `docker compose attach tuva-empi-backend`

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
- Create a bucket:  `aws s3api create-bucket --bucket tuva-health-local`
- Upload a person records file: `aws s3 cp main/tests/resources/tuva_synth/tuva_synth_clean.csv s3://tuva-health-local/raw-person-records.csv`
- POST to the config endpoint: `http -v localhost:8000/api/v1/config splink_settings:=@main/tests/resources/tuva_synth/tuva_synth_model.json potential_match_threshold:=0.5 auto_match_threshold:=1`
- POST to the import endpoint: `http -v localhost:8000/api/v1/person-records/import s3_uri=s3://tuva-health-local/raw-person-records.csv config_id=cfg_1`

#### Processing jobs

- Running the `matching service` so it can process jobs: `make worker`

#### Connecting to the DB directly

When running locally, you can connect to the PGSQL database from a dev container terminal via: `PGPASSWORD=tuva_empi psql -h db -U tuva_empi`

#### Clearing the database

If you'd like to start from scratch with Postgres, either delete the Docker volume where the PGSQL data is stored or drop and recreate the database:

1. `PGPASSWORD=tuva_empi psql -h db -U tuva_empi postgres`
1. `drop database tuva_empi`
1. `create database tuva_empi`

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
