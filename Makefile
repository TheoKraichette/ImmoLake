# Raccourcis docker compose (nécessite `make`).
.DEFAULT_GOAL := help
COMPOSE := docker compose

.PHONY: help up down reset logs ps init test idempotence psql airflow

help: ## Affiche l'aide
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

up: ## Démarre la stack
	cp -n .env.example .env || true
	$(COMPOSE) up -d

down: ## Arrête (conserve les données)
	$(COMPOSE) down

reset: ## Arrête et supprime les volumes
	$(COMPOSE) down -v

logs: ## Suit les logs
	$(COMPOSE) logs -f

ps: ## État des conteneurs
	$(COMPOSE) ps

init: ## Logs de l'init Airflow
	$(COMPOSE) logs -f airflow-init

test: ## Lance pytest
	$(COMPOSE) exec airflow-scheduler pytest tests/ -v

idempotence: ## Rejoue 2x le transform et compare le COUNT
	$(COMPOSE) exec airflow-scheduler airflow dags test immolake_transform_daily 2026-06-17
	$(COMPOSE) exec airflow-scheduler airflow dags test immolake_transform_daily 2026-06-17
	$(COMPOSE) exec postgres-dwh psql -U dwh_user -d immolake -c "SELECT COUNT(*) FROM dwh.fact_biens WHERE dt='2026-06-17';"

psql: ## Shell psql sur le DWH
	$(COMPOSE) exec postgres-dwh psql -U dwh_user -d immolake

airflow: ## Shell bash dans le scheduler
	$(COMPOSE) exec airflow-scheduler bash
