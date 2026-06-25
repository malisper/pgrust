# Audit: backend-utils-adt-jsonb-gin (jsonb_gin.c)

C source: `postgres-18.3/src/backend/utils/adt/jsonb_gin.c` (GIN support for the
`jsonb_ops` and `jsonb_path_ops` operator classes).

Owner crate: `crates/backend-utils-adt-jsonb-gin`
Seam crate: `crates/backend-utils-adt-jsonb-gin-seams`
Shared vocab: `crates/types-jsonb/src/jsonb_gin.rs`

No c2rust run exists for this unit; audited C vs Rust port directly.

## Function inventory (28 functions)

| # | C function | C loc | Port loc (lib.rs) | Verdict | Notes |
|---|-----------|-------|-------------------|---------|-------|
| 1 | `init_gin_entries` | 162 | `gin_entries_init` | MATCH | C doubling bookkeeping = `Vec` growth; `preallocated` is a fallible `try_reserve` capacity hint (bounded by `2*root count`), OOM surfaced as recoverable `ERRCODE_OUT_OF_MEMORY` |
| 2 | `add_gin_entry` | 171 | `gin_entries_add` | MATCH | append + return index; the C `count >= allocated` realloc is `Vec::push` |
| 3 | `gin_compare_jsonb` | 203 | `gin_compare_jsonb` | MATCH | `varstr_cmp(C_COLLATION_OID)` over C collation = unsigned byte compare = `<[u8]>::cmp`; normalized to int32 sign |
| 4 | `gin_extract_jsonb` | 229 | `gin_extract_jsonb` | MATCH | `JB_ROOT_COUNT==0 -> empty`; iterate WJB_KEY(key)/WJB_ELEM(key iff jbvString)/WJB_VALUE(non-key); structural items ignored |
| 5 | `jsonb_ops__add_path_item` | 277 | `jsonb_ops__add_path_item` | MATCH | jpiRoot resets (`items.clear`); jpiKey -> KEY text key; jpiAny/AnyKey/AnyArray/IndexArray -> keyName None; else false |
| 6 | `jsonb_path_ops__add_path_item` | 322 | `jsonb_path_ops__add_path_item` | MATCH | jpiRoot resets hash=0; jpiKey hashes a jbvString; jpiIndexArray/AnyArray unchanged; else false |
| 7 | `make_jsp_entry_node` | 352 | `make_jsp_entry_node` | MATCH | `JSP_GIN_ENTRY` w/ entryDatum = `EntryDatum(bytes)` |
| 8 | `make_jsp_entry_node_scalar` | 363 | `make_jsp_entry_node_scalar` | MATCH | `make_scalar_key` then entry node; threads `Mcx` (numeric key build allocates) |
| 9 | `make_jsp_expr_node` | 369 | (folded) | MATCH | base allocator; folded into the enum constructors below (no separate alloc in the Rust model) |
| 10 | `make_jsp_expr_node_args` | 381 | `make_jsp_expr_node_args` | MATCH | `Logic{and, args}` from a node list |
| 11 | `make_jsp_expr_node_binary` | 394 | `make_jsp_expr_node_binary` | MATCH | `Logic{and, args:[a,b]}` |
| 12 | `jsonb_ops__extract_nodes` | 407 | `jsonb_ops__extract_nodes` | MATCH | parent-chain keys (innermost-first via `.rev()`); string-scalar key_entry table (lax->MAYBE, !last->FALSE, AnyArray/IndexArray->TRUE, Any->MAYBE, else FALSE); MAYBE->OR(key,nonkey) |
| 13 | `jsonb_path_ops__extract_nodes` | 477 | `jsonb_path_ops__extract_nodes` | MATCH | hashes scalar into path hash, one entry node; EXISTS (scalar None) appends nothing |
| 14 | `extract_jsp_path_expr_nodes` | 503 | `extract_jsp_path_expr_nodes` | MATCH | loop over jpiCurrent/jpiFilter/default(add_path_item); unsupported path returns filter nodes only; then extract_nodes. `path` by value (C union by value) = owned `JsonPathGinPath` |
| 15 | `extract_jsp_path_expr` | 563 | `extract_jsp_path_expr` | MATCH | empty->None (full scan); len==1->linitial (no AND); else AND-node |
| 16 | `extract_jsp_bool_expr` | 582 | `extract_jsp_bool_expr` | MATCH | jpiAnd/jpiOr w/ `not ^ (type==jpiAnd)` XOR + larg/rarg None handling; jpiNot flips `not`; jpiExists (not->None); jpiNotEqual->None; jpiEqual scalar-extraction; check_stack_depth at top |
| 17 | `emit_jsp_gin_entries` | 718 | `emit_jsp_gin_entries` | MATCH | ENTRY datum->index via add; recurse Logic args; check_stack_depth at top |
| 18 | `extract_jsp_query` | 747 | `extract_jsp_query` | MATCH | lax from header; opclass callback selection by `path_ops`; ExistsStrategy->path_expr else bool_expr; None or 0 entries -> None (`*nentries=0`); else (entries, root node = `extra_data[0]`) |
| 19 | `execute_jsp_gin_node` | 798 | `execute_jsp_gin_node` | MATCH | AND short-circuits FALSE/tracks MAYBE; OR short-circuits TRUE/tracks MAYBE; ENTRY indexes the (pre-normalized) ternary check vector; unemitted EntryDatum -> Err (C's invalid-node-type elog) |
| 20 | `gin_extract_jsonb_query` | 847 | `gin_extract_jsonb_query` | MATCH | Contains(jsonb)->extract+empty=>ALL; Exists(text)->1 KEY; ExistsAny/All(text[])->per-elem KEY skipping nulls, All+0=>ALL; Jsonpath->extract_jsp_query(pathOps=false)+None=>ALL; else unrecognized strategy Err |
| 21 | `gin_consistent_jsonb` | 928 | `gin_consistent_jsonb` | MATCH | Contains: recheck, all-keys; Exists/ExistsAny: recheck, true; ExistsAll: recheck, all-keys; Jsonpath: recheck, execute!=FALSE when nkeys>0; else Err |
| 22 | `gin_triconsistent_jsonb` | 1012 | `gin_triconsistent_jsonb` | MATCH | Contains/ExistsAll: any FALSE->FALSE; Exists/ExistsAny: start FALSE, any TRUE/MAYBE->MAYBE; Jsonpath: execute, TRUE->MAYBE (never returns TRUE); else Err |
| 23 | `gin_extract_jsonb_path` | 1089 | `gin_extract_jsonb_path` | MATCH | PathHashStack as `Vec` (bottom = C `tail`, NULL parent = index 0); BEGIN pushes parent hash; KEY mixes; ELEM/VALUE mixes+emits uint32+resets to parent; END pops+resets; invalid rc Err |
| 24 | `gin_extract_jsonb_query_path` | 1179 | `gin_extract_jsonb_query_path` | MATCH | Contains->extract_path+empty=>ALL; Jsonpath->extract_jsp_query(pathOps=true)+None=>ALL; else Err |
| 25 | `gin_consistent_jsonb_path` | 1219 | `gin_consistent_jsonb_path` | MATCH | Contains: recheck, all-keys; Jsonpath: recheck, execute!=FALSE; else Err |
| 26 | `gin_triconsistent_jsonb_path` | 1271 | `gin_triconsistent_jsonb_path` | MATCH | Contains: any FALSE->FALSE; Jsonpath: execute, TRUE->MAYBE; else Err |
| 27 | `make_text_key` | 1325 | `make_text_key` | MATCH | overlength (`>JGIN_MAXLENGTH=125`) hashes via `hash_any`->`%08x`->len 8+HASHED bit; builds 4-byte-header varlena (SET_VARSIZE; `[VARHDRSZ]=flag`; payload after) |
| 28 | `make_scalar_key` | 1363 | `make_scalar_key` | MATCH | Null->NULL ""; Bool->BOOL "t"/"f"; Numeric->NUM numeric_normalize; String->KEY/STR; unknown->Err; the `is_key` Asserts are `debug_assert!` |

Helper `numeric_normalize` (numeric.c:1026, ported in-crate): MATCH — special
short-circuit (Infinity/-Infinity/NaN), strip trailing fractional zeroes then a
trailing decimal point, otherwise leave untouched; built on
`set_var_from_num`+`get_str_from_var`. C checks `NUMERIC_IS_SPECIAL` before
`init_var_from_num`; the port decodes first and branches on `sign.is_special()`
(behaviorally identical — a special var has no digits and is never rendered).

## Constants verified (vs headers)

- pg_proc.dat OIDs: gin_compare_jsonb 3480, gin_extract_jsonb 3482,
  gin_extract_jsonb_query 3483, gin_consistent_jsonb 3484,
  gin_triconsistent_jsonb 3488, gin_extract_jsonb_path 3485,
  gin_extract_jsonb_query_path 3486, gin_consistent_jsonb_path 3487,
  gin_triconsistent_jsonb_path 3489 — all match.
- pg_amproc.dat amprocnums: 1 (compare), 2 (extractValue), 3 (extractQuery),
  4 (consistent), 6 (triConsistent) for both opclasses — match.
- jsonb.h strategy numbers: Contains 7, Exists 9, ExistsAny 10, ExistsAll 11,
  JsonpathExists 15, JsonpathPredicate 16 — match `types_jsonb::jsonb`.
- jsonb.h JGINFLAG: KEY 0x01, NULL 0x02, BOOL 0x03, NUM 0x04, STR 0x05,
  HASHED 0x10; JGIN_MAXLENGTH 125 — match `types_jsonb::jsonb`.
- access/gin.h GIN_FALSE 0 / GIN_TRUE 1 / GIN_MAYBE 2; GIN_SEARCH_MODE_ALL 2
  (modeled as the `search_mode_all` bool) — match `types_jsonb`/`types_gin`.

## Seam audit

Owned seam crate `backend-utils-adt-jsonb-gin-seams` declares 9 typed
proc-body seams (one per support proc). All 9 are installed by the owner's
`init_seams()` (verified: 9 declared == 9 `set()`), which contains nothing but
`set()` calls, and `seams-init::init_all()` calls
`backend_utils_adt_jsonb_gin::init_seams()`. No outward seam call performs logic
beyond marshal+delegate.

Outward calls:
- `common_hashfn::hash_bytes` — direct dep (common-hashfn is a leaf), not a seam.
- `backend_utils_misc_stack_depth_seams::check_stack_depth` — genuine external
  (tcop), reached via its owner's seam; panics until that owner lands
  (mirror-PG-and-panic).
- `backend_utils_adt_jsonb_util` (iterator/hash) and
  `backend_utils_adt_jsonpath` (jsp accessors) and `backend_utils_adt_numeric`
  (set_var_from_num/get_str_from_var) — direct deps, no cycle.

Wiring note: there is no GIN by-OID opclass-proc dispatcher in the repo yet
(the GIN core's `gin_extract_value`/`gin_extract_query`/`gin_compare_entries`/
`gin_consistent_*` seams take `FmgrInfo`+`Datum` and are installed by nobody —
unlike GiST's `backend-access-gist-proc`). The jsonb GIN bodies are installed
into typed proc-body seams ready for that future dispatcher to route OIDs
3480/3482-3489 with `Datum` marshaling. This is faithful to the network-gist
model (typed body seams, single installer); the dispatcher is a separate unit.

## Design conformance

- Shared vocab types are real owned data (`Vec`, enum trees), not invented
  handles. The src-idiomatic `ExistsArray(usize)` opaque handle was resolved to
  a real `&[Option<&[u8]>]` (already-deconstructed text array), matching the
  repo's jsonb precedent of deferring `deconstruct_array_builtin` marshaling to
  the fmgr boundary — opacity removed, not introduced.
- Allocating paths thread `Mcx` and return `PgResult`; capacity hints use
  fallible `try_reserve`; OOM is recoverable.
- No statics/atomics/mutexes; no zero-arg getter seams; no locks across `?`.
- `unreachable!()` uses guard a true tagged-union invariant (a `jsonb_ops`
  context's path is always `Items`, `jsonb_path_ops` always `Hash`, set once and
  never changed) — the C accesses the union member directly (UB on mismatch), so
  the Rust assertion is the faithful "cannot happen", not an error path.
- `debug_assert!` for the C `Assert(!is_key)` in make_scalar_key.

## Verdict: PASS

All 28 functions MATCH; helper `numeric_normalize` MATCH. Zero seam findings
(9/9 installed). Zero design-conformance findings. 23 crate tests pass.
`cargo check --workspace` green; no-todo-guard enforcing test green.
