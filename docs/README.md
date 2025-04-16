# Tuva EMPI Docs

## Development

### Installation

#### Prerequisites

1. Install Docker

#### VS Code

1. With VS Code, just open the repository root directory and you should be prompted to open the project in a dev container.
1. Then inside the dev container terminal:
   ```
   > cd docs
   > npm install
   > npm run start
   ```

#### Other IDEs

1. cd `.devcontainer/docs`
1. Build and run the docs dev containers: `docker build -t tuva-empi-docs .`
1. Attach to the docs dev container: `docker run -it --name tuva-empi-docs -v $PWD/../../docs/:/app/docs -p 127.0.0.1:3000:3000 tuva-empi-docs`
1. Then inside the dev container:
   ```
   > cd docs
   > npm install
   > npm run start
   ```
1. `Ctrl-p` followed by `Ctrl-q` allows you to exit the container without stopping it
1. To start the dev container after stopping it: `docker start -i tuva-empi-docs`

### Testing and formatting

1. Run formatter: `npm run format`
1. Run linter: `npm run lint`
