"""URL configuration for tuva_empi project."""

from django.urls import include, path

urlpatterns = [
    path("api/v1/", include("main.urls")),
]

handler404 = "main.views.errors.not_found"
handler500 = "main.views.errors.server_error"
