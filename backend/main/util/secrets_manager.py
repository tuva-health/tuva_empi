import logging
from typing import cast

import boto3  # type: ignore[import-untyped]
import boto3.exceptions  # type: ignore[import-untyped]

logger = logging.getLogger(__name__)


class SecretsManagerClient:
    def __init__(self) -> None:
        self.client = boto3.client("secretsmanager")

    def get_secret(self, secret_arn: str) -> str:
        """Retrieve secret value from AWS Secrets Manager using its ARN."""
        logger.info(
            f"Retrieving secret from AWS Secrets Manager with ARN: {secret_arn}"
        )

        response = self.client.get_secret_value(SecretId=secret_arn)

        if "SecretString" in response:
            return cast(str, response["SecretString"])
        else:
            raise Exception("Unexpected secret format")
