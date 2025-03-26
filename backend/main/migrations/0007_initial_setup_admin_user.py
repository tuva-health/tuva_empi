from django.apps.registry import Apps
from django.db import migrations
from django.db.backends.base.schema import BaseDatabaseSchemaEditor

from main.config import get_config
from main.models import UserRole
from main.services.identity.identity_service import IdentityService


def add_initial_admin_user(apps: Apps, schema_editor: BaseDatabaseSchemaEditor) -> None:
    identity_service = IdentityService()
    admin_email = get_config()["initial_setup"]["admin_email"]
    idp_users = [
        idp_user
        for idp_user in identity_service.get_idp_users()
        if idp_user.email == admin_email
    ]

    if len(idp_users) == 0:
        raise Exception(f"Unable to find IDP user with email {admin_email}")

    if len(idp_users) > 1:
        raise Exception(
            f"Found {len(idp_users)} IDP users with email {admin_email} when only expecting one"
        )

    IdentityService().add_user({"idp_user_id": idp_users[0].id, "role": UserRole.admin})


def remove_initial_admin_user(
    apps: Apps, schema_editor: BaseDatabaseSchemaEditor
) -> None:
    identity_service = IdentityService()
    admin_email = get_config()["initial_setup"]["admin_email"]
    idp_users = [
        idp_user
        for idp_user in identity_service.get_idp_users()
        if idp_user.email == admin_email
    ]

    if len(idp_users) == 0:
        raise Exception(f"Unable to find IDP user with email {admin_email}")

    if len(idp_users) > 1:
        raise Exception(
            f"Found {len(idp_users)} IDP users with email {admin_email} when only expecting one"
        )

    IdentityService().remove_user(idp_user_id=idp_users[0].id)


class Migration(migrations.Migration):
    dependencies = [
        ("main", "0006_user"),
    ]

    operations = [
        migrations.RunPython(
            add_initial_admin_user, reverse_code=remove_initial_admin_user
        )
    ]
