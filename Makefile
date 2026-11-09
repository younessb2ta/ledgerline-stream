UV ?= uv
PY = $(UV) run python
SEED ?= 42
START ?= 2026-11-01
DAYS ?= 1
RATE ?= 600

JAVA_HOME ?= $(HOME)/.local/java/17
MVN ?= $(HOME)/.local/maven/bin/mvn

.PHONY: help install build stack-up stack-down stack-status db-init submit reset \
	produce flush wait-flush replay reconcile restate demo-exactly-once demo-late-data \
	dashboard test lint fmt clean

help:
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  %-20s %s\n", $$1, $$2}'

install: ## Install Python dependencies (uv sync)
	$(UV) sync

build: ## Build the Flink job jar
	cd flink-job && JAVA_HOME="$(JAVA_HOME)" PATH="$(JAVA_HOME)/bin:$$PATH" \
		$(MVN) -q -DskipTests package

stack-up: ## Start Postgres, Kafka, and the Flink cluster
	$(PY) -m stack.postgres up
	$(PY) -m stack.kafka up
	$(PY) -m stack.cluster up

stack-down: ## Stop Flink, Kafka, and Postgres
	$(PY) -m stack.cluster down
	$(PY) -m stack.kafka down
	$(PY) -m stack.postgres down

stack-status: ## Show cluster, database, and broker health
	$(PY) -m stack.cluster status
	$(PY) -m stack.postgres status
	$(PY) -m stack.kafka status

db-init: ## Apply db/schema.sql (idempotent)
	$(PY) -m stack.postgres up

submit: ## Submit the job (builds the jar if stale)
	$(PY) -m stack.cluster submit

reset: ## Cancel jobs, truncate tables, clear checkpoints, recreate one topic partition
	$(PY) -m demo.reset

produce: ## Produce a deterministic, event-time ordered day to Kafka
	$(UV) run eventforge kafka --seed $(SEED) --start $(START) --days $(DAYS) \
		--bootstrap 127.0.0.1:9092 --topic ledger.events --rate $(RATE) --sort

flush: ## Send the end-of-replay control marker (closes open windows)
	$(PY) -m stack.kafka flush

wait-flush: ## Wait until the marker crossed the windows and was checkpointed
	$(PY) -m demo.settle

replay: reset submit ## Full deterministic replay: reset, submit, produce, flush
	$(UV) run eventforge kafka --seed $(SEED) --start $(START) --days $(DAYS) \
		--bootstrap 127.0.0.1:9092 --topic ledger.events --rate $(RATE) --sort
	$(PY) -m stack.kafka flush
	$(PY) -m demo.settle
	$(PY) -m reconciliation.reconcile --seed $(SEED) --start $(START) --days $(DAYS)

reconcile: ## Compare the streaming projection with eventforge ground truth
	$(PY) -m reconciliation.reconcile --seed $(SEED) --start $(START) --days $(DAYS)

restate: ## Recompute windows from every event (adds late data back)
	$(PY) -m reconciliation.reconcile --seed $(SEED) --start $(START) --days $(DAYS) --restate

demo-exactly-once: ## Kill a TaskManager mid-replay, restore, and reconcile
	$(PY) -m demo.exactly_once --seed $(SEED) --start $(START) --days $(DAYS)

demo-late-data: ## 5% of events arrive late; show provisional vs restated
	$(PY) -m demo.late_data --seed $(SEED) --start $(START) --days $(DAYS)

dashboard: ## Run the live Streamlit dashboard
	$(UV) run streamlit run dashboard/app.py

test: ## Run the Python test suite
	$(UV) run pytest

lint: ## Static checks
	$(UV) run ruff check .
	$(UV) run ruff format --check .

fmt: ## Auto-format
	$(UV) run ruff format .
	$(UV) run ruff check --fix .

clean: ## Stop everything and remove local state (checkpoints, database, Kafka data)
	$(PY) -m stack.cluster cancel
	$(PY) -m stack.cluster down
	$(PY) -m stack.kafka down
	$(PY) -m stack.postgres down
	rm -rf .checkpoints artifacts .run .pgdata
