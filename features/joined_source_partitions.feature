Feature: Joined source partitions reconcile every label row
  Scenario: Each frozen source joins a regional label file without losing or duplicating labels
    Given a synthetic regional label file from each frozen source
    When each regional label file is joined to its native text and polygon rows
    Then every label row joins to exactly one text occurrence in its partition
    And the per-status label counts reconcile with the input label rows
    And each polygon geometry row is stored once per partition
