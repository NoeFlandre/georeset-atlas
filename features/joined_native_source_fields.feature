Feature: Native pinned source columns survive the joins
  Scenario: Native pinned columns keep their geometry and link occurrences
    Given native rows declared with the frozen source column names
    When each native source is joined to its labels
    Then description geometry bytes stay exact in the geometry column
    And website and wiki geometry strings stay exact in the geometry column
    And every wiki link occurrence is kept in the polygon document links table
