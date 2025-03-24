import json
import os

from main.util.secrets_manager import SecretsManagerClient


def get_config() -> None:
    secret = SecretsManagerClient().get_secret(
        os.environ["TUVA_MPI_ENGINE_CONFIG_SECRET_ARN"]
    )

    try:
        config = json.loads(secret)
    except Exception as e:
        raise Exception("Unexpected MPI Engine config secret format") from e

    with open(os.environ["DEST_CONFIG_FILE"], "w") as file:
        json.dump(config, file, indent=4)


if __name__ == "__main__":
    get_config()
