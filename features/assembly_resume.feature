Feature: Interruptible global assembly with atomic partition checkpoints
  Scenario: Pause after one new partition and resume without recounting the prefix
    Given a two-partition fixture with one row of each status
    When the builder pauses after one new partition and then resumes
    Then the completed prefix is verified and totals are counted exactly once
