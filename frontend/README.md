# Tuva EMPI Frontend

## Development

### Installation

#### Prerequisites

1. Install Docker

#### VS Code

1. With VS Code, just open the repository root directory and you should be prompted to open the project in a dev container.
1. Then inside the dev container terminal:
   ```
   > cd frontend
   > npm install
   > npm run dev
   ```

#### Other IDEs

1. `cd .devcontainer/tuva-empi-frontend`
1. Build the frontend Docker container: `docker build -t tuva-empi-frontend .`
1. Run the frontend container `docker run --name tuva-empi-frontend -v $PWD:/app -p 127.0.0.1:3000:3000 -it tuva-empi-frontend`
1. Then inside the container:
   ```
   > cd frontend
   > npm install
   > npm run dev
   ```
1. `Ctrl-p` followed by `Ctrl-q` allows you to exit the container without stopping it
1. To start the frontend container after stopping it: `docker start -i tuva-empi-frontend`

### Testing and formatting

1. Run formatter: `npm run format`
1. Run linter: `npm run lint`

### Running the production build

1. `npm run build`
1. `npm run start`

### Troubleshooting

1. Delete `node_modules` folder prior to running `npm install`
1. Remove and restart docker container `docker rm tuva-empi-frontend` before building and starting the container.
1. Ensure docker login `docker login`
