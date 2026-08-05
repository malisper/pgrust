# queryjumble_diff injection sweep (lane p1-queryjumble, 2026-08-01)

Method: plant one defect in the HARNESS C-result marshalling / comparison
(never the crate), run the directed-seed unit rail, require a kill, revert.
Rail: `cargo test -p decoder_fuzz --lib queryjumble_diff::tests::...`
(directed_seeds_fully_compare for jumble planes, clean_querytext_spots for
arm 1).

| id | plant | plane | verdict |
|----|-------|-------|---------|
| I1 | C queryid ^ 1 | P1 queryId | KILLED (QUERYID DIVERGENCE) |
| I2 | drop last C clocation | P2 clocations count | KILLED after fix (see below) |
| I3 | C clocation.location + 1 | P2 location value | KILLED after fix |
| I3b | invert extern_param flag | P2 extern_param | KILLED |
| I4 | C highest_extern_param_id + 1 | P3 | KILLED |
| I5 | invert C has_squashed_lists | P4 | KILLED |
| I6 | C CleanQuerytext offset + 1 | arm-1 | KILLED |
| I7 | corrupt C out_text byte | P0 bridge witness | LIVENESS CONFIRMED (seed run degrades to no-comparison, rail fails loud) |

## The sweep's real catch: TWO dead-plane layers (P2 was fully dead)

First run: I2 and I3 SURVIVED. Root cause chain, both C-parity by design:
1. plain `stringToNode` (both sides) restores every location field to -1
   (stored-rule semantics), and
2. plain `nodeToString` (both sides) also WRITES locations as -1,
so every input reaching the jumble had all locations -1, RecordConstLocation
recorded nothing, and clocations compared empty==empty on 100% of execs —
a plane that "never failed" had never compared anything.

Fix (both C-parity, no behavior invented): the driver reads with the
verbatim `stringToNodeWithLocations` mode on both sides (C: read.c under
DEBUG_NODE_TESTS_ENABLED, compiled into the family; Rust: the previously
unported C API added to the readfuncs crate), and seed texts carry
rehydrated positive locations. Permanent regression guard: the seed rail
asserts non-empty clocations, an observed squash + hep-reset, and
hep == max(paramid) via an asc/desc witness pair.
