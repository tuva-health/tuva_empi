from django.core.management.base import BaseCommand

from main.services.matching.matching_service import MatchingService


class Command(BaseCommand):
    help = "Starts MatchingService"

    def handle(self, *args: str, **options: str) -> None:
        MatchingService().start()
