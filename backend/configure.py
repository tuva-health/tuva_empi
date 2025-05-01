import json
import os

from main.util.secrets_manager import SecretsManagerClient


def load_config() -> None:
    config_path = os.environ["DEST_CONFIG_FILE"]

    if os.path.exists(config_path):
        print(
            f"Config already exists at {config_path}, not pulling from AWS Secrets Manager"
        )
        return

    print("Attempting to pull config from AWS Secrets Manager")

    secret = SecretsManagerClient().get_secret(
        os.environ["TUVA_EMPI_CONFIG_AWS_SECRET_ARN"]
    )

    try:
        config = json.loads(secret)
    except Exception as e:
        raise Exception("Unexpected Tuva EMPI config secret format") from e

    with open(config_path, "w") as file:
        json.dump(config, file, indent=4)


if __name__ == "__main__":
    load_config()
