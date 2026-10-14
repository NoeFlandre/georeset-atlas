Feature: Frozen Atlas source catalog declarations
  Scenario: Declare the recorded source label totals in the catalog
    Given the frozen Atlas source catalog
    When I total each source's recorded label statuses
    Then the catalog declares the documented totals for each immutable input pin
