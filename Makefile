SHELL := /bin/bash
PY = .venv/bin/python

.PHONY: up down lint

lint:
	pre-commit run --all-files
	pre-commit run --all-files

up:
	docker-compose -f docker-compose.yaml up -d

down:
	docker-compose -f docker-compose.yaml down -v
