Feature: helm uninstall cleans up every namespaced object the chart created
  As an operator
  I want uninstall to leave no orphaned objects behind, except explicitly-retained PVCs
  So that AC #8 holds against a real cluster
  (issue #38 AC #8)

  Scenario: Uninstalling a default-values release removes every labelled object except retained PVCs
    Given a Kind cluster with the locally-built pulsusdb image loaded
    And a running pulsusdb release installed with default values
    When I helm uninstall the release
    Then no labelled Deployments, Services, ConfigMaps, or Secrets remain for the release
    And any PersistentVolumeClaims are retained per the chart's documented persistence policy
