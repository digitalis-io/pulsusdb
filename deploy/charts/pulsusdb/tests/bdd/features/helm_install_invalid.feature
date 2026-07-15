Feature: Invalid installs are rejected, either by schema or by the Kubernetes API server
  As an operator
  I want a misconfigured install to fail loudly instead of producing a silently broken deployment
  So that AC #4's negative cases hold against a real cluster, not just `helm template`
  (issue #38 required "install-fails-on-request-over-limit" scenario — round-2 amendment §2:
  Kubernetes' own admission control rejects requests > limits; the chart deliberately does not
  re-implement that check in values.schema.json)

  Scenario: A resources.requests greater than resources.limits install fails at the API server
    Given a Kind cluster with the locally-built pulsusdb image loaded
    When I helm install pulsusdb with "--set-string pulsusdb.resources.requests.cpu=2 --set-string pulsusdb.resources.limits.cpu=1"
    Then the install fails
    And the failure message names a requests/limits conflict

  Scenario: An invalid topology value is rejected before any object reaches the API server
    Given a Kind cluster with the locally-built pulsusdb image loaded
    When I helm install pulsusdb with "--set topology=bogus"
    Then the install fails
    And no namespaced objects are created for the release

  Scenario: topology=cluster with clickhouse.shards=1 is rejected before any object reaches the API server
    Given a Kind cluster with the locally-built pulsusdb image loaded
    When I helm install pulsusdb with "--set topology=cluster --set clickhouse.shards=1"
    Then the install fails
    And no namespaced objects are created for the release
