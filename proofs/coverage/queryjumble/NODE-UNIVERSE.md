# queryjumble node-type universe — C switch vs Rust walker (lane p1-queryjumble)

Oracle: PostgreSQL 18.3 exactly (vendored Stamp-18.3, upstream `62d6c7d3df`).
C tag set extracted from `queryjumblefuncs.switch.c` GENERATED from the
vendored 18.3 headers with the vendored `gen_node_support.pl` (the generated
switch is not checked in upstream); regeneration is reproducible:
`perl src/backend/nodes/gen_node_support.pl <the 23 headers in @all_input_files order>`.

- `c-switch-tags-18.3.txt` — 266 tags with a `case T_x:` in the generated
  jumble switch.
- `rust-walker-tags.txt` — 261 tags referenced as `NodeTag::T_x =>` /
  emissions in `crates/backend/nodes/queryjumble/src/walker.rs`.

## Diff verdict

Rust `jumble_node` match-arm set == C switch set MINUS the 9 tags below.
Counts reconcile exactly: 266 − 9 = 257 = 261 − 4 (the 4 Rust-extra tags are
the List family, which C hand-dispatches in `_jumbleNode` itself, outside the
generated switch: `T_List`/`T_IntList`/`T_OidList`/`T_XidList` — Rust
equivalently hand-handles them in `list()`/`int_list()`/`oid_list()`/
`xid_list()`).

## The 9 C-switch tags absent from the Rust walker — each named, with reason

Reachability frame: `JumbleQuery` runs at parse-analysis time on a `Query`.
Anything only created by the rewriter or planner can never be visited, in C or
in Rust — those switch arms are dead code in C too (gen_node_support.pl emits
an arm for every node type not marked `no_query_jumble`, reachable or not).

| tag | class | reason |
|---|---|---|
| AlterObjectDependsStmt | unconstructible-in-pgrust | Reachable in C via `Query.utilityStmt` (`ALTER FUNCTION/TRIGGER/... DEPENDS ON EXTENSION`). pgrust has NO struct and NO grammar production for it (only the NodeTag constant; `tcop/utility/commandtag.rs:137` routes it to `payload_gap`). If the grammar ever lands, `jumble_gap` panics loudly rather than silently diverging. THE one real future gap — must be revisited when the production is ported. |
| ExtensibleNode | unconstructible-in-pgrust | Extension-registered custom nodes; pgrust has no extensible-node registry. |
| GroupByOrdering | unreachable-post-parse | pathnodes (planner) node; never inside a `Query`. Dead arm in C too. |
| JsonTablePath | unreachable-post-parse | Lives only inside `TableFunc.plan`; generated `_jumbleTableFunc` jumbles only `functype/docexpr/rowexpr/colexprs` (verified), never `plan`. Dead arm in C too. |
| JsonTablePathScan | unreachable-post-parse | Same containment as JsonTablePath. |
| JsonTableSiblingJoin | unreachable-post-parse | Same containment as JsonTablePath. |
| ReturningExpr | unreachable-post-parse | Built by the rewriter (RETURNING OLD/NEW expansion); jumbling precedes rewrite. Dead arm in C too. |
| RTEPermissionInfo | unreachable-post-parse | Lives only in `Query.rteperminfos`, which generated `_jumbleQuery` omits (verified field-by-field; Rust `jumble_query_struct` matches the C field list exactly). Dead arm in C too. |
| WindowFuncRunCondition | unreachable-post-parse | Created by the planner (WindowAgg run conditions); never in a `Query`. Dead arm in C too. |

## Generator completeness bar (harness law for this lane)

The differential harness must assert its constructible-tag set equals the
C switch set minus exactly the 9 named tags above (never a silently smaller
set). Any tag the generator cannot yet construct must appear in an explicit
NOT-YET-CONSTRUCTIBLE list checked by the same assertion, so incompleteness
is loud and enumerated, never implicit.

## Comparison planes (contract)

1. 64-bit queryId (post-DoJumble hash, incl. the 0→1/2 utility fixup).
2. FULL `clocations` array: (location, length, squashed, extern_param) per
   entry, order-sensitive — an off-by-one in RecordConstLocation changes the
   normalized text without changing the hash; hash-only planes are blind here.
3. `highest_extern_param_id` and `has_squashed_lists` (incl. the
   squashed-lists → highest_extern_param_id=0 reset).
