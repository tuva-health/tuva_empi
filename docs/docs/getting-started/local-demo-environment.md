---
id: local-demo-environment
title: "Local demo environment"
hide_title: true
hide_table_of_contents: true
---

# Local demo environment

If you are exploring Tuva EMPI for the first time, you can run it locally in a demo environment:

1. Install [Docker](https://docs.docker.com/) and [Docker Compose](https://docs.docker.com/compose/install/)
1. Clone the Tuva EMPI repository:
   ```shell
   git clone git@github.com:tuva-health/tuva_empi.git
   cd tuva_empi
   ```
1. Create a config file for Docker Compose:
   ```shell
   cp .env.example .env
   ```
1. Create a config file for the Tuva EMPI backend:
   ```shell
   cp backend/config/local.json.example backend/config/local.json
   ```
1. Run Docker Compose:
   ```shell
   docker compose up -d
   ```
1. With a web browser, navigate to `localhost:9000`
1. Login with user `user` and password `test1234`
