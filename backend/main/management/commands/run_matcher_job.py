import argparse

from django.core.management.base import BaseCommand

from main.services.matching.matcher import Matcher


class Command(BaseCommand):
    help = "Processes a single Matching Job"

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument("job_id", type=int, help="ID of the Job to process")

    def handle(self, *args: str, **options: str) -> None:
        job_id = int(options["job_id"])

        Matcher().process_job(job_id)
