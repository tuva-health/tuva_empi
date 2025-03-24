.PHONY: error install install-dev install-all migrate format lint dmypy check test report run run-dev worker configure configure-and-run

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

worker: check
	python manage.py start_match_worker

configure:
	DEST_CONFIG_FILE=config/deployment.json python configure.py

run: migrate
	gunicorn mpi_engine.wsgi:application --bind 0.0.0.0:8000

configure-and-run: configure
	CONFIG_FILE=config/deployment.json $(MAKE) run
