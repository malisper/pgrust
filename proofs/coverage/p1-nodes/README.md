# p1-nodes — nodes-walker family evidence record

Crates: `backend/nodes/copyfuncs` (3,034 SLOC), `backend/nodes/readfuncs`
(1,505), `backend/nodes/outfuncs` (1,305). Branch `proofs/p1-nodes`.
Oracle: PostgreSQL 18.3, vendored "Stamp-18.3", upstream sha `62d6c7d3df`.

**Status: 10M FLEET CONFIRM GREEN** @ `93ae53a4c08a382ca6c936702890b9e4473fced7`,
job `pgrust-fuzz-campaign-1785610008-0718-75797` (2026-08-01, adjudicated from
S3 artifacts): outcome=run, execs 10,000,000/10,000,000, **0 divergences,
0 sanitizer artifacts**, `coverage.lcov` valid (capture-forensics: 11,016/11,016
corpus units replayed), ASAN fake-stack pin verified present in the run script
(run-fuzz-campaign.sh:71). A FIRST submit at `c52267ccdc` crashed-early at
2.09M execs on a REAL finding — the float-format divergence below — which was
fixed, and the rerun at the fixed sha ran the full floor clean.

**Coverage of record (fleet lcov, banked here as
`coverage-93ae53a4c0-*.lcov.gz`):** readfuncs **87.27%** (1474/1689 DA),
outfuncs **82.62%** (1084/1312), copyfuncs **26.32%** (813/3089 — 2,222 of the
uncovered lines are generated.rs copy arms for tags UNREACHABLE from text; the
phase-B direct-construction builder is the designed follow-up, see the census
section). The crates stay `claimed`, NOT `done`: the 100% done-gate needs the
residual-line triage + exception rows (readfuncs/outfuncs) and the phase-B
builder (copyfuncs).

Trailing mutants audit submitted at the same sha:
`pgrust-mutants-audit-1785610021-3c14-84103` (off the critical path).

## The shared node-universe fixture (one fixture, three crates)

`fuzz/core/src/nodesfam_diff.rs`, target `fuzz/fuzz_targets/nodesfam_diff.rs`.
The input language is node text — the outfuncs serialization language — so ONE
corpus drives all three crates. Per exec, both sides:

```
read(text) -> node          readfuncs   vs  stringToNode
out(node)  -> text'         outfuncs    vs  nodeToString
copy(node) -> node2         copyfuncs   vs  copyObject
out(node2) -> text''
read(text') -> node3; out(node3) -> text'''
C only: equal(node, copy)   equalfuncs (structural witness)
```

Compared planes, every exec where both sides accept:

| plane | claim |
|---|---|
| P1 | `text'` bytes identical C vs Rust |
| P2 | `text'' == text'` on EACH side (copy is out-identical) |
| P3 | `text''' == text'` on EACH side (out→read round-trip is stable) |
| P4 | C `equal(node, copy)` holds |
| P5 | accept/reject verdicts agree; packed sqlstate compared on structured errors |

P2/P3 are the SELF-CHECKING oracle the charter asked for: they hold
independently of the C side, so they still bite on inputs where the C
comparison is carved.

Arm 1 (`run_value_nodes`) builds value/list nodes programmatically
(String/Integer/Float/Boolean/List/IntList/OidList + an escaping-heavy String
arm), rust-outs them, and feeds the rendered text to C — that is how the
out/copy arms for value tags get exercised, since the scoped read port cannot
reach them from text.

## Tag-set completeness (the load-bearing proof)

All sets are parsed from the SOURCE OF TRUTH at test time, never hand-listed:
the C sets from the GENERATED switch files (`gen_node_support.pl` output under
`fuzz/core/csrc/nodesfam/gen/`) plus the hand-written value/list arms of the C
`.c` files; the Rust sets from the shipped crate sources.

| crate | C tags/labels | port dispatches | complement |
|---|---|---|---|
| copyfuncs | 331 | **321** | 10, ALL NO-VOCAB, each named |
| outfuncs | 387 | 87 | 300 out-of-charter |
| readfuncs | 316 labels | 80 | 236 out-of-charter |

Enforced by `copyfuncs_tag_census_is_exact`, `outfuncs_tag_census_is_exact`,
`readfuncs_label_census_is_exact`: zero EXTRA tags on the Rust side, and the
complement must EQUAL the recorded ledger — an unrecorded gap FAILS the test.

**Tags that cannot be constructed (all 10, copyfuncs), with reasons** — no
`pub struct` exists in the `types_nodes` vocabulary, so this lane's generator
cannot build them at all:

- planner-internal (`pathnodes.h`), never serialized to a catalog column:
  `PathKey`, `RestrictInfo`, `SpecialJoinInfo`, `PlaceHolderInfo`,
  `GroupByOrdering`
- relcache-internal: `ForeignKeyCacheInfo`
- extension surface, registry absent on BOTH sides: `ExtensibleNode`,
  `CustomScan`
- utility statements not in the vocabulary: `AlterExtensionContentsStmt`,
  `AlterObjectDependsStmt`

One level below the tag census, `RANGETBLENTRY`'s reader is a 10-way switch on
rtekind, so **every RTEKind branch has its own C-validated seed**
(`every_rtekind_branch_has_a_seed_and_is_compared`): 9/10 fully compared, and
`RTE_RESULT` (8) is a NAMED port gap (its arm is unported; RTE_RESULT is legal
in a stored view rule, so this is a genuine TODO). Every one of the 80
port-dispatched read labels also has a seed
(`every_port_read_label_has_a_seed`).

## pgrust defects found and FIXED in-lane

1. **All three walkers were missing `check_stack_depth`** where C calls it
   (`outfuncs.c:733` outNode, `copyfuncs.c:185` copyObjectImpl,
   `readfuncs.c:578` parseNodeString). Deep nesting crashed the process where
   C raises 54001. Fixing this required splitting `stack_depth` into
   `stack_depth_core` (the guard) + `stack_depth` (the GUC half, re-exporting
   the core) because the GUC half depends on `guc`, which depends
   transitively on these very crates. Witness:
   `deep_nesting_hits_the_guard_on_both_sides` (60k-deep, both sides 54001).
2. **Empty/whitespace node string panicked** where C returns the NULL node
   (`stringToNode("")` → `nodeRead(NULL,0)` → NULL → prints `<>`). Witness:
   `empty_and_whitespace_input_is_the_null_node`.
3. **Silent i32 wrap in the value-node path (DATA CORRUPTION)**: any
   digit-leading token became an Integer via a truncating `as i32`, so
   `9992999999` built an Integer node holding **1403065407** while C builds a
   Float node printing `9992999999`. The port now applies C
   `nodeTokenType`'s own rule (`strtoint` over the UNSIGNED magnitude). This
   also resolved a text-invisible tag divergence at `-2147483648`, where C
   builds a Float for the exact text outfuncs writes for an Integer node
   holding INT32_MIN. Witnesses:
   `out_of_range_integer_token_does_not_wrap`,
   `int32_min_token_follows_cs_magnitude_rule`.

## pgrust defect #4 — float-field formatting (found by the FLEET CONFIRM)

`WRITE_FLOAT_FIELD` in C prints through Ryu shortest-decimal
(`double_to_shortest_decimal_buf`); the port used Rust `{}` Display, whose
NOTATION differs for large exponents: `startup_cost` 4.44e113 printed as 115
expanded digits vs C's `4.4444444444444444e+113` — catalog text written by
pgrust differed from C. FIXED via the verified byte-identical ryu port
(`crates/common/ryu`) at all three float write sites (SUBPLAN costs, RTE
enrtuples). Witness: `float_fields_use_shortest_decimal` (+ a NaN P4 carve:
C's own equalfuncs compares floats with `==`, so `equal(node, copy)` is FALSE
for NaN fields in real PostgreSQL too).

## Divergence of record — MATCH-OR-FIX RULING OWED

**`GroupingSet.content`: the port picks the list FLAVOR from a sibling field.**
`out_grouping_set` emits `(i ...)` iff `kind == GROUPING_SET_SIMPLE` and calls
`out_list` otherwise, while C's `outNode` picks the flavor from the LIST'S
ACTUAL TAG. The mismatch cuts both ways, and the fuzzer found both directions:

| input | C | pgrust |
|---|---|---|
| `:kind 1 :content (14)` | `(14)` | `(i 14)` |
| `:kind 0 :content (i 14)` | `(i 14)` | `(14)` |

Root cause is one line of the port: the flavor is inferred from a sibling field
instead of carried by the value. Both mismatched spellings are UNREACHABLE from
PG-written text — the rewriter stores an `IntList` exactly when kind is SIMPLE,
NIL (`<>`) for EMPTY, and a List of GroupingSet nodes for ROLLUP/CUBE/SETS — so
the compared domain excludes them while the writer-produced combinations
round-trip identically on both sides (seed `seed-groupingset-intlist`). A
proper fix means carrying the IntList-vs-List distinction in the vocabulary,
which is a port change outside this lane's mandate — hence a RULING, not a
silent carve. Test:
`groupingset_content_list_marker_divergence_is_recorded` pins BOTH directions
and fails if the port's behavior changes or the gate stops covering them.

## Oracle / harness defects found (would have produced false greens)

- **`nf_`/`pg_nf_` prefixes COLLIDE with the netfam family.** macOS ld64 only
  WARNS on duplicate symbols; GNU ld on the fleet hard-errors. Renamed
  `ndf_`/`pg_ndf_`. Caught before any submit.
- **libfuzzer-sys' panic hook aborts on EVERY panic**, pre-empting
  `catch_unwind`, so the scoped ports' chartered loud panics killed the
  process instead of being classified. Hook now stays silent only inside the
  chartered region; real divergences still print and abort.
- **Per-thread guard arming.** C's `stack_base_ptr` is a process static while
  Rust's is thread-local, so process-once arming made the guard fire on 100%
  of inputs — the campaign's known shallow-plane failure mode, caught by six
  tests going red the moment the process used more than one test thread.
- **`-funsigned-char` oracle pin.** outfuncs' datum writer prints each byval
  byte as `(int) *s++` off a `char *`, so plain-char signedness decides
  whether `0xFF` prints `-1` (macOS) or `255` (fleet Linux + the pgrust u8
  port). Without the pin every high datum byte is a false OUT-TEXT
  divergence.
- **Oracle `palloc` must RAISE over MaxAllocSize, not abort.**
  `(b 0 00800000000000)` is a Bitmapset needing a ~1 TB word array; PG refuses
  with 54000 and so does pgrust, while the arena shim aborted the process (an
  un-comparable verdict).
- **`custom_shapes` self-poisoning.** Shapes are learned from the corpus, and
  libFuzzer wrote a misspelled-field BOOLEXPR INTO the corpus; the gate then
  accepted it as legal and the harness reported its own poisoning as a
  divergence. Learning is now restricted to curated `seed-*` files.

## THE C READERS ARE NOT HARDENED — the compared-domain gate

Catalog node text is written by C's own outfuncs and therefore TRUSTED: only a
handful of shapes elog, and `READ_*_FIELD` **skips the field-name token
without comparing it** (`/* skip :fldname */`). Four distinct crash shapes were
found in the verbatim C oracle, none of them pgrust defects:

1. truncated field list — `{CREATESTMT :relation <>}` (1 of 13 fields) SEGVs
   inside `_readCreateStmt`
2. `readDatum` reads its bytes with `atoi(token)` and **no NULL check**, so a
   short `[...]` payload SEGVs inside `strtol`
3. a stray token inside a block shifts the stream by one, so C reads a field
   NAME as a VALUE and walks off into a NULL deref
4. an out-of-domain enum is stored raw and echoed (`format_type 5`), or read
   into a branch whose fields don't match (`rtekind 6` with a relation body)

So the compared domain is **writer-producible node text**, enforced by a
recursive-descent gate over a faithful `pg_strtok` port. What the gate models,
all derived mechanically from the vendored C:

- field-name sequences per label, from the GENERATED reader bodies
- per-field VALUE kinds (`READ_INT/UINT/OID/BOOL/CHAR/FLOAT/ENUM/NODE/
  BITMAPSET_FIELD`), because C's numeric macros use `atoi`/`atooid`/`strtod`
  and silently ignore trailing garbage (`atoi("0`") == 0)
- enum DOMAINS from a generated table (`gen_enum_domains.py` over the vendored
  header closure, 156 enums; `*` = unmodelled, stays permissive)
- a NODE field's value is only `<>`, `{`, or `(` (what `outNode` writes)
- `readDatum`'s payload grammar (length, `[`, 8 tokens byval / length byref,
  `]`), a NULL Const's `constvalue` being exactly `<>`
- CUSTOM (hand-written) readers: strict `:field value` alternation, per-field
  kind checks, and shape keys that include DISCRIMINANT values (rtekind, kind)
- bare list-element/top-level tokens restricted to what outfuncs emits

Each rule has a liveness test naming its witness input.

## Recorded carve classes (each with a liveness test; disjointness tested)

| class | what | why not a divergence |
|---|---|---|
| OutOfCharter | node label outside the scoped port | chartered loud panic; label not in the catalog node universe |
| NonNull (14) | `read_node("f")?.expect(...)` where C's `READ_NODE_FIELD` accepts NULL | mandatory child in C's own node contract; unreachable from PG-written text |
| EnumDomain (24 validators) | port validates enum domains, C casts blindly | **subsumed by the gate**; and all 24 validators accept EXACTLY their C enum's declared set (proved by `port_enum_validators_equal_the_c_domains`) |
| ValueToken | bare Boolean/Float/BitString tokens (C `nodeTokenType`: `b` OR `x`, and numeric non-i32 → T_Float) | value nodes outside the read charter; their out/copy arms ARE compared via arm 1 |
| Unported | `(x ...)` XID lists + out-of-charter arms | named scope gaps |

`carve_classes_are_disjoint` asserts exactly one counter moves per witness, so
no class hides another. `nonnull_carves_match_the_port`,
`enum_carves_match_the_port`, `custom_reader_labels_match_the_c_source` and
`unported_shape_carve_is_live_and_singular` keep every table EQUAL to what the
port/C actually contain, so a table cannot silently drift.

## LOCAL numbers

- 41 harness tests green (`cargo test -p decoder_fuzz nodesfam`), 1 ignored
  (the corpus-hygiene helper).
- Corpus: 1,594 committed inputs, 100 curated `seed-*` (one per port read label,
  one per RTEKind, plus escaping/boundary/NULL-vs-empty-list seeds), all
  validated against the compiled C oracle before commit; 1,577 of 1,594 reach a
  full P1..P4 comparison (17 are carve witnesses).
- **CLEAN LOCAL LEG (number of record): 19,677,252 execs, ZERO divergences**,
  under `ASAN_OPTIONS=detect_stack_use_after_return=0`, libFuzzer cov 7,584
  edges / ft 17,045, corpus grown to 3,190 inputs, ~14.4k exec/s, rss 683 MB.
  The run ended on the harness's own wall-clock timeout ("run interrupted"),
  NOT on a crash. Earlier legs each ended on an artifact; every one was
  decoded, and all of them were the harness/oracle defects, gate holes and
  pgrust defects listed above — each fixed before the next leg.
  **This is a LOCAL number on macOS/aarch64. It is NOT the campaign's 10M
  CONFIRM, which must run on the fleet at the pinned sha with the ASAN option
  above (FLEET-QUEUE.md).**
- Coverage: NOT captured. `fuzz/cov-export.sh nodesfam_diff` is the local
  recipe; the coverage of record is the fixed fleet capture (see
  FLEET-QUEUE.md step 3).

## Reuse (sibling lane p1-queryjumble)

`queryjumblefuncs` walks the same node universe. Reusable as-is:

- `fuzz/core/csrc/nodesfam/` — the whole verbatim C family + `assemble.sh`
  (`gen_node_support.pl` also emits `queryjumblefuncs.{funcs,switch}.c`;
  `assemble.sh` currently deletes them, so drop that one `rm` line)
- the fixture, gate and carve machinery in `fuzz/core/src/nodesfam_diff.rs`
  (`pg_strtok_all`, `expected_fields`, `enum_domains`, `is_well_formed`,
  `parse_block`/`parse_list`/`parse_value`, `classify_panic`)
- `fuzz/corpus/nodesfam_diff/` — the 99 curated seeds
- `fuzz/gen_nodesfam_seeds.py` — per-label seed generation + C validation
- the census pattern in `nodesfam_diff_tests.rs` (parse the generated switch,
  diff against the port, require the complement to equal a recorded ledger)
