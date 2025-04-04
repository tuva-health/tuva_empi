# Tuva EMPI

## Development

### Installation

#### Prerequisites

1. Install Docker

#### Setup .env and Tuva EMPI config file

The `.env` is used for local development in order to configure Docker Compose services. Meanwhile, the Tuva EMPI config file (e.g. `backend/config/local.json`) is used for configuring the Tuva EMPI backend.

To get started, you can copy the examples and edit them as needed (the default configuration in the example files should just work as is):

1. `cp .devcontainer/.env.example .devcontainer/.env`
1. `cp backend/config/local.json.example backend/config/local.json`

Alternatively, there is a more secure approach to storing these configuration files. Both of these files may contain sensitive information (e.g. when testing with AWS Cognito) and are thus added to the .gitignore by default. However, as an added precaution, it's nice to keep them outside of the repo (in case someone modifies the .gitignore or you revert to a commit before the files were ignored). In order to do this, you can copy both files to a location outside the repo and symlink them (git doesn't follow symlinks):

- Create a directory to store secrets: `mkdir -p ~/.secret/tuva_empi`
- Move `.env` and symlink it:
  1. `cp .devcontainer/.env.example ~/.secret/tuva_empi/.env`
  1. `chmod 0600 ~/.secret/tuva_empi/.env`
  1. `ln -s ~/.secret/tuva_empi/.env .devcontainer/.env`
- Move `local.json.example` and symlink it:
  1. `cp backend/config/local.json.example ~/.secret/tuva_empi/local.json`
  1. `chmod 0600 ~/.secret/tuva_empi/local.json`
  1. `ln -s ~/.secret/tuva_empi/local.json backend/config/local.json`


#### VS Code

1. With VS Code, just open the repository root directory and you should be prompted to open the project in a dev container. Otherwise you can open the command palette and run: `Dev Containers: Rebuild and Reopen in Container`
1. You have the choice of choosing a backend or frontend dev container.

#### Other IDEs

1. cd `.devcontainer`
1. Build and run the dev Docker containers: `docker compose up -d`
1. Attach to the either the frontend or backend app container, for example: `docker attach tuva-empi-backend-1`
1. `Ctrl-p` followed by `Ctrl-q` allows you to exit the container without stopping it
1. To start the app container after stopping it: `docker start -i tuva-empi-backend-1`
1. To start and attach to the app container after restarting your system: `docker compose start` then `docker compose attach tuva-empi-backend-1`

#### Next steps

See README.md files in `frontend` and `backend` directories.

## Setting up Git hooks

After cloning the repository, run the following command to set up the Git hooks:

```sh
./scripts/setup-hooks.sh
```

This will install the pre-commit hook that runs linting before each commit.
