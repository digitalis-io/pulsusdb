Feature: helm upgrade rolls pods that self-reconcile their own schema
  As an operator
  I want an upgrade to a schema-affecting values change to roll pods safely
  So that AC #7 holds and the required "upgrade-time schema gating" scenario is proven
  (architect round-3 code-review disposition, task-manager final ruling #3): there is no
  install-path init Job (round-2 amendment §1) — the ConfigMap's checksum/config
  annotation is what actually triggers the rollout, and readiness alone gates traffic
  during and after it; replacement pods self-reconcile their own schema before serving
  (issue #38 AC #7)

  Scenario: Upgrading a schema-affecting value changes the checksum/config annotation, rolls pods, and each replacement self-reconciles before reaching Ready
    Given a Kind cluster with the locally-built pulsusdb image loaded
    And a running pulsusdb release installed with default values
    When I helm upgrade the release with "--set pulsusdb.config.retention_days=14"
    Then the upgrade succeeds
    And the pod template's checksum/config annotation changed from before the upgrade
    And every pod in the release reaches Ready within the timeout budget
    And the release status is deployed
    And replacement pods were not Ready until their readiness probe passed
