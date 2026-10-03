# Convenience wrappers around the local-only Grafana demo stack
# (deploy/e2e/compose.grafana.yaml, deploy/e2e/compose.tier.yaml) — not
# used by CI or the `pulsus-e2e` harness, which drive compose directly
# (see e2e/src/engine.rs).

PULSUS_E2E_ROOT ?= $(CURDIR)
COMPOSE_FILES := -f docker-compose.yaml -f deploy/e2e/compose.single.yaml -f deploy/e2e/compose.grafana.yaml -f deploy/e2e/compose.tier.yaml
COMPOSE_PROJECT := pulsus-e2e-grafana
# compose.grafana.yaml's `firehose.environment.WINDOW_START` is a
# required interpolation var (no default), so every compose invocation
# needs it set, not just `up` — `down`/`logs` parse the same file and
# fail the same "required variable ... is missing a value" error
# otherwise, even though neither command actually starts firehose with
# it. Computed fresh each time; only `up`'s value is ever observed by a
# running container.
COMPOSE := PULSUS_E2E_ROOT=$(PULSUS_E2E_ROOT) WINDOW_START=$$(date -u +%Y-%m-%dT%H:%M:%SZ) \
	docker compose $(COMPOSE_FILES) -p $(COMPOSE_PROJECT)

.PHONY: grafana-up grafana-down grafana-logs

## Build and start the full local test env: firehose (logs/metrics/
## traces) plus the three-tier (frontend/middletier/backend) distributed-
## tracing demo, with Loki/Tempo/Prometheus datasources against pulsusdb.
## WINDOW_START is computed fresh on every invocation so firehose ships
## current data instead of its default month-old backdated window (see
## compose.grafana.yaml's `firehose.environment.WINDOW_START` comment).
grafana-up:
	$(COMPOSE) up -d --build

## Tear down the demo stack, including volumes.
grafana-down:
	$(COMPOSE) down -v

## Follow logs across the demo stack.
grafana-logs:
	$(COMPOSE) logs -f
