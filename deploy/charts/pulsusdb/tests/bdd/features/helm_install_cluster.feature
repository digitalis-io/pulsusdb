Feature: Install PulsusDB with topology=cluster (sharded bundled ClickHouse)
  As an operator
  I want a 3-shard cluster install to bring up every shard and wire pulsusdb correctly
  So that AC #3's real fan-out contract is proven against a live API server, not just rendered templates
  (issue #38 AC #6)

  Scenario: 3-shard cluster install reaches Ready on every shard and the helm test hook passes
    Given a Kind cluster with the locally-built pulsusdb image loaded
    When I helm install pulsusdb with "--set topology=cluster --set clickhouse.shards=3"
    Then the install succeeds
    And every pod in the release reaches Ready within the timeout budget
    And 3 distinct ClickHouse shard StatefulSets are Ready
    And the bundled helm test hook exits successfully
