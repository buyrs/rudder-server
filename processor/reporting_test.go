package processor

// Scenario 1 - 1 group, start from the initial stage, "" -> PU1, 100 total

// Scenario 2 - 1 group, start from an intermediate stage, PU1 -> PU2, 100 total, 50 successful, 20 Failed, verify diff

// Scenario 3 - 2 groups (group1: PU1 -> PU2 -> PU3, group2: PU1 -> PU3), each group starts with 100, and have 10 Failures & 10 discards after each stage
// | Group1 | Group2 | Success | Fail | Diff |
