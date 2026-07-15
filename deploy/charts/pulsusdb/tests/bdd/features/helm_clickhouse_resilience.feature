Feature: PulsusDB pods stay running-but-unready during a ClickHouse outage, never restarted
  As an operator
  I want a prolonged ClickHouse outage to degrade readiness only, never trigger a restart storm
  So that the probe contract (issue #38 plan amendment §1 — liveness/startupProbe are always
  plain TCP, only readiness ever depends on /ready) holds against a real kubelet, not just
  rendered YAML. Also proves reconcile-retry: once ClickHouse comes back, the same pod
  (never restarted) self-reconciles and becomes Ready again on its own
  (issue #38 task-manager final ruling #3 — required scenarios)

  Scenario: A pod survives a prolonged ClickHouse outage unready, then self-heals when ClickHouse returns
    Given a Kind cluster with the locally-built pulsusdb image loaded
    And a running pulsusdb release installed with default values
    When the bundled ClickHouse StatefulSet is scaled to 0 replicas
    And I wait for the pulsusdb pod to report NotReady
    Then the pulsusdb pod has not restarted
    When the bundled ClickHouse StatefulSet is scaled back to 1 replica
    Then the pulsusdb pod reports Ready again within the timeout budget
    And the pulsusdb pod still has not restarted

  # Round-2 code-review test gap #5, tightened by round-3 disposition for
  # determinism: the scenario above proves resilience to an outage a pod
  # discovers *after* it was already healthy. This scenario instead
  # exercises the pod's very first reconcile attempt —
  # crates/pulsus-server/src/serve.rs's ensure_schema_then_connect retry
  # loop starting cold, before ClickHouse has ever been reachable at all.
  # ClickHouse is scaled to zero (and confirmed absent) *before* the
  # pulsusdb pod is ever created — not raced against `--wait=false` — by
  # installing with `pulsusdb.replicaCount=0` first (so no pulsusdb pod
  # exists to race against ClickHouse's own startup at all), bringing
  # ClickHouse down and confirming it has zero running pods, and only then
  # scaling pulsusdb up to create its first pod. This makes "ClickHouse
  # was already down when pulsusdb's pod was created" a precondition the
  # test establishes and confirms, not a timing bet.
  Scenario: A pod created after ClickHouse was already confirmed down stays NotReady without restarting, then becomes Ready once ClickHouse appears
    Given a Kind cluster with the locally-built pulsusdb image loaded
    When I helm install pulsusdb with pulsusdb.replicaCount=0 and the bundled ClickHouse StatefulSet is confirmed scaled to 0 replicas
    And I scale the pulsusdb Deployment up to 1 replica
    Then the pulsusdb pod stays NotReady without restarting for a sustained window
    When the bundled ClickHouse StatefulSet is scaled back to 1 replica
    Then the pulsusdb pod reports Ready again within the timeout budget
    And the pulsusdb pod still has not restarted
