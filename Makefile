SHELL := /bin/bash

.PHONY: help up down restart logs ps dashboard score clean

help:
	@echo "Available targets:"
	@echo "  make up         - Start full stack (docker compose up -d --build)"
	@echo "  make down       - Stop stack"
	@echo "  make restart    - Restart full stack"
	@echo "  make logs       - Follow docker compose logs"
	@echo "  make ps         - Show service status"
	@echo "  make dashboard  - Start stack and auto-open Grafana dashboard"
	@echo "  make score      - Run CV project scoring script"
	@echo "  make clean      - Stop stack and remove orphans"

up:
	docker compose up -d --build

down:
	docker compose down

restart: down up

logs:
	docker compose logs -f --tail=200

ps:
	docker compose ps

dashboard:
	./setup.sh

score:
	./cv_project_score.sh

clean:
	docker compose down --remove-orphans
