from django.urls import path

from main.views.config import create_config
from main.views.data_sources import get_data_sources
from main.views.health_check import health_check
from main.views.matches import create_match
from main.views.person_records import import_person_records
from main.views.persons import get_person, get_persons
from main.views.potential_matches import get_potential_match, get_potential_matches
from main.views.users import get_users, update_user

urlpatterns = [
    path("health-check", health_check, name="health_check"),
    path("users", get_users, name="get_users"),
    path("users/<str:id>", update_user, name="update_user"),
    path("config", create_config, name="create_config"),
    path("person-records/import", import_person_records, name="import_person_records"),
    path("data-sources", get_data_sources, name="get_data_sources"),
    path("potential-matches", get_potential_matches, name="get_potential_matches"),
    path("potential-matches/<str:id>", get_potential_match, name="get_potential_match"),
    path("matches", create_match, name="create_match"),
    path("persons", get_persons, name="get_persons"),
    path("persons/<str:id>", get_person, name="get_person"),
]
