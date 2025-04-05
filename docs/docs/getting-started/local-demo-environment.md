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
   ```
1. Run Docker Compose:
   ```shell
   cd tuva_empi
   docker compose up -d
   ```
1. With a web browser, navigate to `localhost:9000`
1. Login with user `user` and password `test1234`
