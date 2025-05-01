import logging
import os

from django.core.management.base import BaseCommand

from main.config import get_config
from main.services.matching.matcher import Matcher

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Processes a single Matching Job"

    def handle(self, *args: str, **options: str) -> None:
        version = get_config()["version"]
        expected_version = os.environ["TUVA_EMPI_EXPECTED_VERSION"]

        logger.info(f"Matching Job version: {version}")

        if version != expected_version:
            raise Exception(
                f"Matching Job version ({version}) doesn't equal expected version ({expected_version}). Refusing to run."
            )

        Matcher().process_next_job()
