# Audit: backend-utils-adt-multirangetypes

- **Verdict:** PASS (after one fix-and-re-audit round)
- **Date:** 2026-06-13
- **Model:** Claude Fable 5 (Opus 4.8 1M context)
- **C source:** `../pgrust/postgres-18.3/src/backend/utils/adt/multirangetypes.c` (2927 lines) + `src/include/utils/multirangetypes.h`
- **c2rust reference:** `../pgrust/c2rust-runs/backend-utils-adt-multirangetypes/`
- **Port:** `crates/backend-utils-adt-multirangetypes/src/{lib,serialize_core,typcache_io,operators,setops_ordering_agg}.rs`

## Method

Independent re-derivation done at assembly time after merging the four family
sub-branches and syncing main. Enumerated every function definition (incl.
statics/inline helpers) from the C source — every C function maps to a
directly-named port counterpart (verified by name cross-check; 94 C functions,
zero unmapped). Read C + port for each; spot-checked the cores in detail
(canonicalize, contains_multirange_internal, get_multirange_io_data,
constructor2, hash_multirange, unnest). Constants and the hash mixer verified
against headers (`common/hashfn.h`: `hash_uint32(k) =
UInt32GetDatum(hash_bytes_uint32(k))`), not from memory.

## Per-function table

Dispatch wrappers (`*_PG_FUNCTION_ARGS`) and their `*_internal` helpers are
listed together where the port fused the thin wrapper into one fn.

| C function | port location | verdict | notes |
|---|---|---|---|
| multirange_in | typcache_io.rs:248 | MATCH | full parse state machine; malformed-literal errors carry ERRCODE_INVALID_TEXT_REPRESENTATION |
| multirange_out | typcache_io.rs:411 | MATCH | empty → `{}`; delegates member range out via range seams |
| multirange_recv | typcache_io.rs:447 | MATCH | pq_getmsgint32/bytes/end mirrored; insufficient-data errors |
| multirange_send | typcache_io.rs:486 | MATCH | length-prefixed member encoding |
| get_multirange_io_data | typcache_io.rs:109 | MATCH | fn_extra cache → per-call typcache lookup (optimization, not semantics); not-multirange elog (XX000) plain; missing recv/send fn fixed (see Findings) |
| multirange_canonicalize | serialize_core.rs:754 | MATCH | sort (stable merge_sort_by w/ fallible comparator) + skip-empty + adjacent/before/overlap merge; identical output to qsort_arg path |
| multirange_get_typcache | typcache_io.rs:89 | SEAMED+MATCH | inward seam; calls lookup_type_cache_entry(TYPECACHE_MULTIRANGE_INFO) |
| multirange_size_estimate | serialize_core.rs:272 | MATCH | header + items + flags + boundaries offsets |
| write_multirange_data | serialize_core.rs:331 | MATCH | per-range bound serialize, align, item/flag arrays |
| make_multirange | serialize_core.rs:383 | MATCH | inward seam; canonicalize + size + write |
| multirange_get_bounds_offset | serialize_core.rs:559 | MATCH | item-array off/len decode |
| multirange_get_range | serialize_core.rs:594 | MATCH | deserialize i-th member range |
| multirange_get_bounds | serialize_core.rs:645 | MATCH | inward seam; flags + lower/upper bound decode |
| multirange_get_union_range | serialize_core.rs:708 | MATCH | empty→empty range; else first-lower..last-upper |
| multirange_deserialize | serialize_core.rs:727 | MATCH | materialize all member ranges |
| make_empty_multirange | serialize_core.rs:416 | MATCH | make_multirange with 0 ranges |
| multirange_constructor0/1/2 | serialize_core.rs:541/511/438 | MATCH | nargs/null/dims>1 (CARDINALITY_VIOLATION)/type-mismatch (XX000)/per-elem null (NULL_VALUE_NOT_ALLOWED) all faithful |
| range_bounds_overlaps | operators.rs:57 | MATCH | |
| range_bounds_contains | operators.rs:81 | MATCH | |
| multirange_bsearch_match | operators.rs:102 | MATCH | generic over comparator key; lo/hi bisection identical |
| multirange_elem_bsearch_comparison | operators.rs:136 | MATCH | |
| multirange_contains_elem(_internal) | operators.rs:177 | MATCH | |
| elem_contained_by_multirange | (dispatch) | MATCH | arg-swap of contains_elem |
| multirange_range_contains_bsearch_comparison | operators.rs:191 | MATCH | |
| multirange_contains_range(_internal) | operators.rs:216 | MATCH | |
| range_contains_multirange(_internal) | operators.rs:241 | MATCH | |
| range_contained_by_multirange | (dispatch) | MATCH | |
| multirange_contained_by_range | (dispatch) | MATCH | |
| multirange_contains_multirange(_internal) | operators.rs:267 | MATCH | tandem walk; ++i1 pre-increment semantics preserved |
| multirange_contained_by_multirange | (dispatch) | MATCH | |
| range/multirange overlaps (+bsearch_comparison, +internal) | operators.rs:319/341/361 | MATCH | |
| range_overleft/overright_multirange(_internal) | operators.rs:409/427 | MATCH | |
| multirange_overleft/overright_range/multirange | (dispatch) | MATCH | empty handling matches |
| range/multirange before/after (+internal) | operators.rs:445/464/484 | MATCH | |
| range/multirange adjacent (+internal) | operators.rs:501 | MATCH | |
| multirange_eq/ne(_internal) | operators.rs:538/570 | MATCH | |
| multirange_empty | operators.rs:584 | MATCH | |
| multirange_lower/upper | operators.rs:590/608 | MATCH | empty→error path preserved |
| multirange_lower_inc/upper_inc/lower_inf/upper_inf | operators.rs:627–667 | MATCH | |
| multirange_unnest | operators.rs:682 | MATCH | SRF materialized as in-order Vec of member ranges (cross-call SRF machinery is funcapi concern) |
| multirange_union | setops_ordering_agg.rs:66 | MATCH | |
| multirange_minus(_internal) | setops_ordering_agg.rs:92/112 | MATCH | |
| multirange_intersect(_internal) | setops_ordering_agg.rs:197/217 | MATCH | |
| range_merge_from_multirange | setops_ordering_agg.rs:289 | MATCH | empty→empty; single→get_range; else first-lower..last-upper via make_range |
| multirange_cmp | setops_ordering_agg.rs:315 | MATCH | type-match check (elog), tandem bound compare, shorter-comes-first |
| multirange_lt/le/ge/gt | setops_ordering_agg.rs:361–388 | MATCH | sign of cmp |
| hash_multirange | setops_ordering_agg.rs:414 | MATCH | hash_uint32==hash_bytes_uint32 (UInt32GetDatum no-op); flags^lower, rotl(1), ^upper; (r<<5)-r+rh wrapping; element-type hash-proc fallback via lookup_range_elem_hash_proc |
| hash_multirange_extended | setops_ordering_agg.rs:479 | MATCH | seeded variant; rotate_high_and_low_32bits mixer |
| range_agg_transfn / finalfn | setops_ordering_agg.rs:572/587 | MATCH | ArrayBuildState of range datums → canonicalized multirange |
| multirange_agg_transfn | setops_ordering_agg.rs:608 | MATCH | unnest input multirange members into state |
| multirange_intersect_agg_transfn | setops_ordering_agg.rs:631 | MATCH | running intersection |

Helpers (multirange_item_get_offlen/has_off, alignment, offsets, palloc0,
elem_type, is_space, bsearch comparators, pg_rotate_left32,
rotate_high_and_low_32bits, could_not_identify_hash_fn): MATCH — verified against
the C macros/inlines and `common/hashfn.h`.

## Seam audit

Owned inward seam crate: `backend-utils-adt-multirangetypes-seams` (covers
multirangetypes.c). Declares 4 seams — `multirange_get_typcache`,
`make_multirange`, `multirange_get_bounds`, `datum_get_multirange_type_p` — and
all 4 are installed by `init_seams()` (lib.rs:55–58), which contains only
`set()` calls. `seams-init::init_all()` calls
`backend_utils_adt_multirangetypes::init_seams()` (verified). No `set()` outside
the owner.

Outward seam calls are all genuine cross-unit dependency cuts (would cycle if
direct), each a thin marshal+delegate with no branching/construction in the seam
path: rangetypes-seams (range_compare, range_*_internal, range_cmp_bounds,
range_serialize/deserialize, make_range, datum_get_range_type_p), typcache-seams
(lookup_type_cache_entry, lookup_range_elem_hash_proc), lsyscache-seams
(get_type_io_data), fmgr-seams (function_call1_coll), arrayfuncs-seams
(array_get_ndim/elemtype, deconstruct_array, ArrayBuildState ops),
format-type-seams (format_type_be), common-hashfn-seams
(hash_bytes_uint32[_extended]).

No function body was replaced by a seam call to relocate logic — every C
function's logic lives in this crate.

## Design conformance

No invented opacity; allocating paths take `Mcx` + return `PgResult`; error
paths use builders with explicit SQLSTATE; no shared statics for per-backend
state; the fn_extra IO cache correctly degrades to a per-call lookup (no
registry side table). The stable-merge substitution for `qsort_arg` is
documented and behavior-identical for the comparable key set.

### Seam contract reconcile (assembly)

The sync onto main surfaced a seam-collision in `typcache-seams`: the family
branches had introduced `lookup_type_cache_range` / `lookup_type_hash_proc`,
duplicating main's canonical `lookup_type_cache_entry` /
`lookup_range_elem_hash_proc` (identical signatures/return types). Resolved by
dropping the family-introduced duplicates and rewiring all multirange call sites
to main's canonical names. No remaining divergent seams.

## Findings (fixed this round)

1. **DIVERGES → fixed.** `get_multirange_io_data` missing-binary-{recv,send}
   function error dropped both the C SQLSTATE and the type-name rendering: it
   raised a plain `PgError::error` (defaults to XX000) with the bare numeric OID
   instead of `ereport(ERROR, errcode(ERRCODE_UNDEFINED_FUNCTION), errmsg("...
   %s", format_type_be(...)))`. Restored `ERRCODE_UNDEFINED_FUNCTION` and the
   `format_type_be` type name (transient context, matching the existing
   `could_not_identify_hash_fn` pattern). Re-audited: MATCH.

## Conclusion

Every function MATCH (or SEAMED per the seam rules). Zero outstanding seam or
design findings. **PASS.**
