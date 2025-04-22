---
id: overview
title: "Architecture"
hide_title: true
hide_table_of_contents: true
sidebar_position: 1
---

# Architecture

## Core

Tuva EMPI is a distributed web application that consists of 3 core pieces:

1. frontend
2. API
3. matching service

### Frontend

The Tuva EMPI frontend is a Typescript, NextJS and React single-page application. For state management, we are currently using [zustand](https://github.com/pmndrs/zustand).

### API

The Tuva EMPI API is a Python and Django application. It serves a JSON HTTP API that is used by the frontend to perform the core functionality that Tuva EMPI provides. The API can also be used directly.

### Matching Service

The Tuva EMPI matching service is a part of the same backend source as the API, but is deployed separately. It is responsible for processing matching jobs. A matching job auto-matches new person records and also groups person records that potentially match. It's a bulk operation that is run each time you load a new person record source file into Tuva EMPI. The matching service waits for new jobs to be created and then runs the job in a job runner.

#### Supported Job Runners

Currently, the only supported job runner is the `ProcessJobRunner` which runs jobs in a local child process.

## Identity Provider

Tuva EMPI depends on an external identity provider for it's authentication needs. The identity provider handles sign-in and also serves as the source-of-truth for users that Tuva EMPI recognizes. It's recommended that the identity provider used for Tuva EMPI is only used for Tuva applications and not shared with other applications in your environment. That's because we may depend on features of the identity provider (e.g. JWT claims) in an application-specific way.

While an external identity provider handles authentication, the Tuva EMPI API handles authorization (permissions). To give a user access to Tuva EMPI, you must first add them to your identity provider, then via the Tuva EMPI admin API, you can grant them permission to access Tuva EMPI.

### Supported Identity Providers

Currently, the supported identity providers are:

- [Keycloak](https://www.keycloak.org/)
- [AWS Cognito](https://aws.amazon.com/cognito/)

Please see [Production Environment](../getting-started/production-environment.md) for details on how to set those up to work correctly with Tuva EMPI.

## Storage Connectors

In order to support getting data into and out of Tuva EMPI, it supports connecting to external storage providers.

### Supported Storage Connectors

Currently, the only supported storage provider is AWS S3.
