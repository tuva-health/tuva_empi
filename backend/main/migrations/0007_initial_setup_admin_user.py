from django.apps.registry import Apps
from django.db import migrations
from django.db.backends.base.schema import BaseDatabaseSchemaEditor

from main.config import get_config
from main.models import User, UserRole
from main.services.identity.identity_service import IdentityService


def add_initial_admin_user(apps: Apps, schema_editor: BaseDatabaseSchemaEditor) -> None:
    identity_service = IdentityService()
    admin_email = get_config()["initial_setup"]["admin_email"]
    users = [user for user in identity_service.get_users() if user.email == admin_email]

    if len(users) == 0:
        raise Exception(f"Unable to find IDP user with email {admin_email}")

    if len(users) > 1:
        raise Exception(
            f"Found {len(users)} IDP users with email {admin_email} when only expecting one"
        )

    IdentityService().update_user_role(users[0].id, UserRole.admin)


def remove_initial_admin_user(
    apps: Apps, schema_editor: BaseDatabaseSchemaEditor
) -> None:
    identity_service = IdentityService()
    admin_email = get_config()["initial_setup"]["admin_email"]
    users = [user for user in identity_service.get_users() if user.email == admin_email]

    if len(users) == 0:
        raise Exception(f"Unable to find IDP user with email {admin_email}")

    if len(users) > 1:
        raise Exception(
            f"Found {len(users)} IDP users with email {admin_email} when only expecting one"
        )

    User.objects.get(id=users[0].id).delete()


class Migration(migrations.Migration):
    dependencies = [
        ("main", "0006_user"),
    ]

    operations = [
        migrations.RunPython(
            add_initial_admin_user, reverse_code=remove_initial_admin_user
        )
    ]
