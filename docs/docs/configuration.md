---
id: configuration
title: "Configuration"
hide_title: true
hide_table_of_contents: true
sidebar_position: 4
---

# Configuration

## Backend

Configuring the Tuva EMPI backend is primarily done with a JSON file:

```js
{
    // A name for the configuration environment (often the name of the environment you are deploying to) (required)
    "env": "local",
    // PGSQL connection details (required)
    "db": {
        "user": "tuva_empi",
        "password": "tuva_empi",
        "name": "tuva_empi",
        "host": "db",
        "port": "5432"
    },
    // Django specific settings, see https://docs.djangoproject.com/en/5.2/topics/settings/ (required)
    "django": {
        "debug": false,
        "secret_key": "django-insecure-$@j@kbe1r&e!g*%kz(#mwje1z+6$fs24m6h4rukhkmsi))l8vg",
        "allowed_hosts": [".localhost", "127.0.0.1", "[::1]", "oauth2-proxy"]
    },
    // Identity provider settings (required)
    "idp": {
        // Can either be 'keycloak' or 'aws-cognito' (required)
        "backend": "keycloak",
        // AWS Cognito settings (required if backend is set to "aws-cognito")
        "aws_cognito": {
            // The AWS Cognito user pool ID
            "cognito_user_pool_id": "",
            // The header that contains the JWT
            "jwt_header": "X-Forwarded-Access-Token",
            // The JWKS URL for retrieving JWT public keys
            "jwks_url": "",
            // The OAuth client ID
            "client_id": ""
        },
        // Keycloak settings (required if backend is set to "keycloak")
        "keycloak": {
            // The URL of the Keycloak server
            "server_url": "http://keycloak:8080",
            // The Keycloak realm for Tuva EMPI
            "realm": "test-realm",
            // The header that contains the JWT
            "jwt_header": "X-Forwarded-Access-Token",
            // The expected audience claim in the JWT
            "jwt_aud": "account",
            // The JWKS URL for retrieving JWT public keys
            "jwks_url": "http://keycloak:8080/realms/test-realm/protocol/openid-connect/certs",
            // The OAuth client ID
            "client_id": "test-client",
            // The OAuth client secret
            "client_secret": "insecure-1234"
        }
    },
    // Bootstrap settings (required)
    "initial_setup": {
        // The email of a user that already exists in the identity provider. This user will be
        // granted admin permissions in Tuva EMPI on first launch.
        "admin_email": "user@example.com"
    }
}
```

See [local.json.example](https://github.com/tuva-health/tuva_empi/blob/main/backend/config/local.json.example) for a valid JSON example.

There are also two other environment variables:

- `TUVA_EMPI_CONFIG_FILE`: Path to Tuva EMPI backend config file
- `TUVA_EMPI_CONFIG_AWS_SECRET_ARN`: ARN of AWS Secrets Manager secret containing the config file (`TUVA_EMPI_CONFIG_FILE` takes priority)
- `TUVA_EMPI_MATCHING_SERVICE__K8S_JOB_RUNNER__JOB_IMAGE`: The image used for the Matching Job when using the `K8sJobRunner`
