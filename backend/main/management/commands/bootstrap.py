from django.core.management.base import BaseCommand

from main.config import get_config
from main.models import UserRole
from main.services.identity.identity_service import IdentityService


class Command(BaseCommand):
    help = "Bootstrap the database with an initial admin user"

    def handle(self, *args: str, **options: str) -> None:
        identity_service = IdentityService()
        admin_email = get_config()["initial_setup"]["admin_email"]
        users = [
            user for user in identity_service.get_users() if user.email == admin_email
        ]

        if len(users) == 0:
            raise Exception(f"Unable to find IDP user with email {admin_email}")

        if len(users) > 1:
            raise Exception(
                f"Found {len(users)} IDP users with email {admin_email} when only expecting one"
            )

        IdentityService().update_user_role(users[0].id, UserRole.admin)

        print(f"Added initial admin user with email {admin_email}")
