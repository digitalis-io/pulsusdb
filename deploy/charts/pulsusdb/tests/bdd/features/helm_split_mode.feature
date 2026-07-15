Feature: Split-mode (independent writer/reader tiers) renders and installs correctly
  As an operator running a horizontally-scaled deployment
  I want pulsusdb.split.enabled=true to produce independently-scaled, independently-Ready
  writer and reader tiers against a live cluster
  So that the split axis (orthogonal to topology) is proven end to end
  (issue #38 architect plan — the two orthogonal axes; required "split-mode render/install" scenario)

  Scenario: A split-mode install brings up independent writer and reader Deployments, both Ready
    Given a Kind cluster with the locally-built pulsusdb image loaded
    When I helm install pulsusdb with "--set pulsusdb.split.enabled=true"
    Then the install succeeds
    And every pod in the release reaches Ready within the timeout budget
    And independent writer and reader Deployments both exist with the requested replica counts
    And the bundled helm test hook exits successfully
