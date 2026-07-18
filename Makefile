# Convenience wrappers around the local-only Grafana demo stack
# (deploy/e2e/compose.grafana.yaml) — not used by CI or the `pulsus-e2e`
# harness, which drive compose directly (see e2e/src/engine.rs).

PULSUS_E2E_ROOT ?= $(CURDIR)
COMPOSE_FILES := -f docker-compose.yaml -f deploy/e2e/compose.single.yaml -f deploy/e2e/compose.grafana.yaml
COMPOSE_PROJECT := pulsus-e2e-grafana

.PHONY: grafana-up grafana-down grafana-logs

## Build and start the Grafana + firehose demo stack (logs/metrics/traces
## with Loki/Tempo/Prometheus datasources against pulsusdb). WINDOW_START
## is computed fresh on every invocation so firehose ships current data
## instead of its default month-old backdated window (see
## compose.grafana.yaml's `firehose.environment.WINDOW_START` comment).
grafana-up:
	PULSUS_E2E_ROOT=$(PULSUS_E2E_ROOT) WINDOW_START=$$(date -u +%Y-%m-%dT%H:%M:%SZ) \
		docker compose $(COMPOSE_FILES) -p $(COMPOSE_PROJECT) up -d --build

## Tear down the demo stack, including volumes.
grafana-down:
	PULSUS_E2E_ROOT=$(PULSUS_E2E_ROOT) docker compose $(COMPOSE_FILES) -p $(COMPOSE_PROJECT) down -v

## Follow logs across the demo stack.
grafana-logs:
	PULSUS_E2E_ROOT=$(PULSUS_E2E_ROOT) docker compose $(COMPOSE_FILES) -p $(COMPOSE_PROJECT) logs -f
