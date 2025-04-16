.PHONY: error install install-dev install-all makemigrations migrate bootstrap format lint dmypy check test report run-dev matching-service-dev configure run matching-service

error:
	@echo "Available commands: install, migrate, format, test, run"
	@exit 2

install:
	pip install -r requirements.txt

install-dev:
	pip install -r requirements-dev.txt

install-all: install-dev install

makemigrations:
	python manage.py makemigrations $(app)

migrate:
	python manage.py migrate $(app)

bootstrap:
	python manage.py bootstrap

format:
	ruff format
	ruff check --fix

lint:
	ruff format --check
	ruff check

dmypy:
	- dmypy start

check: dmypy
	dmypy check .

test: check
	coverage run manage.py test

test-single:
	@if [ -z "$(test)" ]; then \
		echo "Error: test variable is not set. Use 'make test-single test=<test_name>'"; \
		exit 1; \
	fi
	python manage.py test $(test)

report:
	coverage report -m --omit="main/tests/*"

run-dev: check
	python manage.py runserver 0.0.0.0:8000

matching-service-dev: check
	python manage.py run_matching_service

configure:
	@env DEST_CONFIG_FILE="$(TUVA_EMPI_BACKEND_CONFIG_FILE)" python configure.py

run:
	tuva-empi-api

matching-service:
	tuva-empi-matching-service
