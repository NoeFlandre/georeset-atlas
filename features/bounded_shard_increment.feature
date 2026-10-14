Feature: Bounded resumable shard increments over a synthetic FakeHub fixture
  Scenario: Rerun a completed row-group shard without new FakeHub range requests
    Given a synthetic label Parquet served by FakeHub under the declared labels root
    When the shard command writes row group 1 projecting the label column within budget
    And the shard command runs again with the same arguments
    Then the rerun verifies the shard and issues no new FakeHub range requests

  Scenario: Preserve an orphan shard instead of overwriting it
    Given a synthetic label Parquet served by FakeHub under the declared labels root
    And a shard was already written but its checkpoint was lost
    When the shard command writes row group 1 projecting the label column within budget
    Then the command exits with the conflict code and keeps the orphan shard unchanged
