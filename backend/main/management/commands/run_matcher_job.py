from django.core.management.base import BaseCommand

from main.services.matching.matcher import Matcher


class Command(BaseCommand):
    help = "Processes a single Matching Job"

    def handle(self, *args: str, **options: str) -> None:
        Matcher().process_next_job()
