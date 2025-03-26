"""Django settings for tuva_empi project.

Generated by 'django-admin startproject' using Django 5.1.2.

For more information on this file, see
https://docs.djangoproject.com/en/5.1/topics/settings/

For the full list of settings and their values, see
https://docs.djangoproject.com/en/5.1/ref/settings/
"""

import logging
import os
import sys
from pathlib import Path

import django_stubs_ext

from main.config import get_config

django_stubs_ext.monkeypatch()

config = get_config()

print("Tuva EMPI config profile: ", config["env"])

# Build paths inside the project like this: BASE_DIR / 'subdir'.
BASE_DIR = Path(__file__).resolve().parent.parent


# Quick-start development settings - unsuitable for production
# See https://docs.djangoproject.com/en/5.1/howto/deployment/checklist/

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = config["django"]["secret_key"]

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = config["django"].get("debug", False)

print("Debug mode enabled: ", DEBUG)

ALLOWED_HOSTS: list[str] = config["django"].get(
    "allowed_hosts", [".localhost", "127.0.0.1", "[::1]", "oauth2-proxy"]
)

# Application definition

INSTALLED_APPS = [
    # "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    # "django.contrib.sessions",
    # "django.contrib.messages",
    # "django.contrib.staticfiles",
    "rest_framework",
    "corsheaders",
    "main",
]

REST_FRAMEWORK = {
    "EXCEPTION_HANDLER": "main.views.errors.exception_handler",
    "DEFAULT_AUTHENTICATION_CLASSES": [
        "main.views.auth.jwt.JwtAuthentication",
    ],
    "DEFAULT_PERMISSION_CLASSES": ["main.views.auth.permissions.IsMemberOrAdmin"],
    "DEFAULT_PARSER_CLASSES": [
        "rest_framework.parsers.JSONParser",
    ],
    "DEFAULT_RENDERER_CLASSES": [
        "rest_framework.renderers.JSONRenderer",
    ],
}

MIDDLEWARE = [
    "corsheaders.middleware.CorsMiddleware",
    "django.middleware.security.SecurityMiddleware",
    # "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    # "django.contrib.auth.middleware.AuthenticationMiddleware",
    # "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

ROOT_URLCONF = "tuva_empi.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

WSGI_APPLICATION = "tuva_empi.wsgi.application"


# Database
# https://docs.djangoproject.com/en/5.1/ref/settings/#databases

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": "tuva_empi",
        "USER": config["db"]["user"],
        "PASSWORD": config["db"]["password"],
        "HOST": config["db"]["host"],
        "PORT": config["db"]["port"],
    }
}


# Password validation
# https://docs.djangoproject.com/en/5.1/ref/settings/#auth-password-validators

AUTH_PASSWORD_VALIDATORS = [
    {
        "NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.MinimumLengthValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.CommonPasswordValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.NumericPasswordValidator",
    },
]


# Internationalization
# https://docs.djangoproject.com/en/5.1/topics/i18n/

LANGUAGE_CODE = "en-us"

# NOTE: Don't change this
TIME_ZONE = "UTC"

USE_I18N = True

# NOTE: Don't change this
USE_TZ = True


# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/5.1/howto/static-files/

STATIC_URL = "static/"

# Default primary key field type
# https://docs.djangoproject.com/en/5.1/ref/settings/#default-auto-field

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"


class IsInfoFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        return record.levelno == logging.INFO


class IsNotInfoFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        return record.levelno != logging.INFO


LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "verbose": {
            "format": "{levelname} {asctime} {process} {name} {message}",
            "style": "{",
            "datefmt": "%Y-%m-%dT%H:%M:%S%z",
        },
    },
    "filters": {
        "is_info": {
            "()": IsInfoFilter,
        },
        "is_not_info": {
            "()": IsNotInfoFilter,
        },
    },
    "handlers": {
        "console-stderr": {
            "class": "logging.StreamHandler",
            "formatter": "verbose",
            "stream": sys.stderr,
            "filters": ["is_not_info"],
        },
        "console-stdout": {
            "class": "logging.StreamHandler",
            "formatter": "verbose",
            "stream": sys.stdout,
            "filters": ["is_info"],
        },
    },
    "root": {
        "handlers": ["console-stdout", "console-stderr"],
        "level": "INFO",
    },
    "loggers": {
        "django": {
            "handlers": ["console-stderr"],
            "level": os.getenv("DJANGO_LOG_LEVEL", "INFO"),
            "propagate": False,
        },
    },
}

CORS_ALLOWED_ORIGINS = config["django"].get("cors_allowed_origins", [])
CORS_ALLOW_ALL_ORIGINS = True if config.get("env") in {"local", "ci"} else False

if CORS_ALLOW_ALL_ORIGINS:
    print("**WARNING** CORS_ALLOW_ALL_ORIGINS set to True")
else:
    print(f"CORS allowed origins: {CORS_ALLOWED_ORIGINS}")
