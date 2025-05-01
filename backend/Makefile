.PHONY: error install install-dev install-all makemigrations migrate bootstrap format lint dmypy check test test-single dmypy-ci check-ci test-ci report run-dev matching-service-dev configure run matching-service

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
	- @env TUVA_EMPI_CONFIG_FILE="config/local.test.json" dmypy start

check: dmypy
	dmypy check .

test: check
	@env TUVA_EMPI_CONFIG_FILE="config/local.test.json" coverage run manage.py test

test-single: check
	@if [ -z "$(test)" ]; then \
		echo "Error: test variable is not set. Use 'make test-single test=<test_name>'"; \
		exit 1; \
	fi
	@env TUVA_EMPI_CONFIG_FILE="config/local.test.json" python manage.py test $(test)

dmypy-ci:
	- @env TUVA_EMPI_CONFIG_FILE="config/ci.json" dmypy start

check-ci: dmypy-ci
	dmypy check .

test-ci: check-ci
	@env TUVA_EMPI_CONFIG_FILE="config/ci.json" coverage run manage.py test

report:
	coverage report -m --omit="main/tests/*"

run-dev: check
	python manage.py runserver 0.0.0.0:8000

matching-service-dev: check
	python manage.py run_matching_service

configure:
	@env DEST_CONFIG_FILE="$(TUVA_EMPI_CONFIG_FILE)" python configure.py

run:
	./tuva-empi-backend api

matching-service:
	./tuva-empi-backend matching-service
