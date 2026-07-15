Feature: Install PulsusDB with the default (single-topology, bundled ClickHouse) values
  As an operator
  I want a default `helm install` to produce a fully working single-node stack
  So that the chart's happy path is proven end to end, not just rendered
  (issue #38 AC #5, and the required "install-default-ready" scenario)

  Scenario: Default install reaches Ready and serves an ingest-then-query round trip
    Given a Kind cluster with the locally-built pulsusdb image loaded
    When I helm install pulsusdb with default values
    Then every pod in the release reaches Ready within the timeout budget
    And the bundled helm test hook exits successfully
    And a log line ingested through the collector is queryable back through the PulsusDB API
