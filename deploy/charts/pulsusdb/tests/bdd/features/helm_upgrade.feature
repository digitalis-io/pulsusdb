Feature: helm upgrade rolls pods, and the schema is not theirs to build
  As an operator
  I want an upgrade to a schema-affecting values change to roll pods safely
  So that AC #7 holds and the required "upgrade-time schema gating" scenario is proven
  (architect round-3 code-review disposition, task-manager final ruling #3): the
  ConfigMap's checksum/config annotation is what triggers the rollout, and readiness
  alone gates traffic during and after it.

  **The schema is `schema/schema.sql`, applied by the schema Job.** A replacement pod
  builds nothing — it reads a database somebody else built and stays unready while one
  is absent. So a values change that affects the schema rolls the pods, and whether the
  schema itself changed is a separate question the operator answers by running the Job,
  which drops the database and builds it again.

  Scenario: Upgrading a schema-affecting value changes the checksum/config annotation, rolls pods, and every replacement reaches Ready
    Given a Kind cluster with the locally-built pulsusdb image loaded
    And a running pulsusdb release installed with default values
    When I helm upgrade the release with "--set pulsusdb.config.log_rollup_resolution=10s"
    Then the upgrade succeeds
    And the pod template's checksum/config annotation changed from before the upgrade
    And every pod in the release reaches Ready within the timeout budget
    And the release status is deployed
    And replacement pods were not Ready until their readiness probe passed

  # Issue #506. The schema Job's name is a digest of its whole `spec`: a value
  # the Job does not carry leaves it alone, and a value it does carry replaces
  # it. Before that, both upgrades were refused with `field is immutable`.
  Scenario: Upgrading a value the schema Job does not carry succeeds
    Given a Kind cluster with the locally-built pulsusdb image loaded
    And a running pulsusdb release installed with default values
    When I helm upgrade the release with "--set pulsusdb.config.reader.cache_ttl=30s"
    Then the upgrade succeeds
    And the schema Job is the same Job as before the upgrade
    And every pod in the release reaches Ready within the timeout budget
    And the release status is deployed

  Scenario: Upgrading the schema Job's own resources replaces the Job and the release reaches Ready
    Given a Kind cluster with the locally-built pulsusdb image loaded
    And a running pulsusdb release installed with default values
    When I helm upgrade the release with "--set initJob.resources.limits.memory=2Gi"
    Then the upgrade succeeds
    And the schema Job was replaced by one whose memory limit is "2Gi"
    And every pod in the release reaches Ready within the timeout budget
    And the release status is deployed
