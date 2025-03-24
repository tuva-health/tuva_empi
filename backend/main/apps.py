from django.apps import AppConfig
from django.conf import settings


class MainConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "main"

    def ready(self) -> None:
        if settings.TIME_ZONE != "UTC":
            raise RuntimeError(
                f"TIME_ZONE must be set to 'UTC', but found {settings.TIME_ZONE}"
            )

        if not settings.USE_TZ:
            raise RuntimeError(
                f"USE_TZ must be set to True, but found {settings.USE_TZ}"
            )
