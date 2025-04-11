---
id: frontend-installation
title: "Frontend Installation"
hide_title: true
hide_table_of_contents: true
sidebar_position: 3
---

# Tuva EMPI Frontend

## Development

### Installation

Inside the dev Docker container, you can run:

1. `cd frontend`
1. `npm install`
1. `npm run dev`

Then, on the host, in a web browser, visit `localhost:9000`

### Testing and formatting

1. Run formatter: `npm run format`
1. Run linter: `npm run lint`

### Running the production build

1. `npm run build`
1. `npm run start`

### Troubleshooting

1. Delete `node_modules` folder prior to running `npm install`
1. Remove and restart docker container `docker rm tuva-empi-frontend-1` before building and starting the container.
1. Ensure docker login `docker login`
