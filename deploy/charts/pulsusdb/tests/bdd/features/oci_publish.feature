Feature: The chart packages and round-trips through an OCI registry
  As a release engineer
  I want `helm package` + `helm push` + `helm pull` to work against a throwaway OCI registry
  So that the packaging/push mechanics are proven before `chart-publish` ever touches ghcr.io
  (issue #38 architect plan Testing Approach: "OCI publish test", AC #11's round-trip verify)

  Scenario: A packaged chart pushes to a local OCI registry and pulls back with matching metadata
    Given a throwaway OCI registry running locally
    When I helm package the chart
    And I helm push the packaged chart to the throwaway registry
    Then the push succeeds
    When I helm pull the chart back from the throwaway registry
    Then the pulled Chart.yaml name and version match the packaged chart
