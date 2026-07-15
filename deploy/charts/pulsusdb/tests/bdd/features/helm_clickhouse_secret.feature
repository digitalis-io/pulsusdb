Feature: The bundled ClickHouse is never passwordless, and its password never leaks into the ConfigMap
  As a security-conscious operator
  I want a default install's ClickHouse credential to be a generated Secret, never plaintext/passwordless
  So that AC-adjacent review finding [medium] (issue #38 plan amendment §4) holds against a live cluster
  (issue #38 task-manager final ruling #3 — required scenario: "bundled-ClickHouse Secret
  non-default + redaction")

  Scenario: A default install generates a non-empty ClickHouse password Secret, absent from the ConfigMap, and never logged
    Given a Kind cluster with the locally-built pulsusdb image loaded
    When I helm install pulsusdb with default values
    Then a ClickHouse credentials Secret exists with a non-empty, non-default password
    And the release's ConfigMap contains no password value anywhere in its data
    And the pulsusdb pod's own logs never contain the ClickHouse password
    And GET /config on the pulsusdb API redacts the ClickHouse password
