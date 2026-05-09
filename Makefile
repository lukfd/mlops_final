# Copyright (c) 2026 Luca and Pam. All rights reserved.

.PHONY: help up stop down restart status logs logs-follow start-api clean kind-load-consumer kind-get-nodes kind-apply-consumer kind-scale-consumer kind-build-consumer kind-refresh-consumer

COMPOSE_FILE := docker-compose.yml
SERVE_API_SCRIPT := scripts/serve_api.py
DEFAULT_MODEL_PATH := models/iris_model.pkl
API_HOST ?= 0.0.0.0
API_PORT ?= 8000
API_MODEL_PATH ?= $(DEFAULT_MODEL_PATH)

up:
	docker compose -f $(COMPOSE_FILE) up -d --build

stop:
	docker compose -f $(COMPOSE_FILE) stop

down:
	docker compose -f $(COMPOSE_FILE) down

restart: stop up

status:
	docker compose -f $(COMPOSE_FILE) ps

logs:
	docker compose -f $(COMPOSE_FILE) logs --tail 100

logs-follow:
	docker compose -f $(COMPOSE_FILE) logs -f

start-api:
	python $(SERVE_API_SCRIPT) --host $(API_HOST) --port $(API_PORT) --model-path $(API_MODEL_PATH)

clean:
	docker compose -f $(COMPOSE_FILE) down -v

kind-load-consumer:
	docker compose -f $(COMPOSE_FILE) exec -T kind-provisioner \
		kind load docker-image inference-consumer:latest --name ai-cluster

kind-build-consumer:
	docker compose -f $(COMPOSE_FILE) exec -T docker-host \
		docker build -t inference-consumer:latest /opt/project/consumer

kind-refresh-consumer: kind-build-consumer kind-load-consumer kind-apply-consumer
	@echo "Consumer refreshed and deployed."

kind-get-nodes:
	docker compose -f $(COMPOSE_FILE) exec airflow-webserver \
		kubectl --kubeconfig /opt/project/kubeconfig.yaml get nodes

kind-apply-consumer:
	docker compose -f $(COMPOSE_FILE) exec airflow-webserver \
		kubectl --kubeconfig /opt/project/kubeconfig.yaml apply -f /opt/project/k8s/deployment.yaml

kind-scale-consumer:
	docker compose -f $(COMPOSE_FILE) exec airflow-webserver \
		kubectl --kubeconfig /opt/project/kubeconfig.yaml scale deployment inference-consumer --replicas=3

write-up:
	pandoc ./WRITEUP.md -o hw3.pdf
