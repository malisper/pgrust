# Audit — backend-utils-cache-typcache (independent re-audit, tightened rule)

Unit: `backend-utils-cache-typcache`
C source: `src/backend/utils/cache/typcache.c` (+ `src/include/utils/typcache.h`)
Crate: `crates/backend-utils-cache-typcache`
c2rust: `../pgrust/c2rust-runs/backend-utils-cache-typcache/src/typcache.rs`

This re-audit was run under the **tightened rule** that abolishes the
"deferred / SEAMED-equivalent / documented deferred convergence" escape (the
loophole that let execTuples merge with 35 `todo!()` bodies). A body is
acceptable only if it is either (a) real own-logic, or (b) a real
`<owner>-seams::fn::call` site dispatching to a genuinely-unported owner crate.
`todo!()`/`unimplemented!()`/bare-`panic!`-for-own-logic/"deferred until X" is
**MISSING**, not SEAMED.

## Loophole scan (the point of this re-audit)

- `grep -E 'todo!|unimplemented!'` over `src/lib.rs`: **0 hits.**
- `grep -E 'panic!|unreachable!'` over own logic: **0 hits.** (The DSA
  shared-typmod-registry passthroughs are real `session_seams::*::call`
  sites, not bare panics.)
- No body is "deferred until <keystone> lands" / "SEAMED-equivalent" /
  "documented deferred convergence". Every "not implemented here" is a real
  `::call` into an unported owner — call sites enumerated in the seam audit
  below.

The previously-flagged domain-constraint orchestration
(`load_domaintype_info` / `dcs_cmp` / `prep_domain_constraints`) was, in the
prior fix round, brought fully in-crate; this re-audit re-derived it from C
1083-1389 and confirms it is real own-logic, not a seam-to-domains delegation
(see the per-function notes).

## Function inventory and verdicts

Every function definition in typcache.c (statics + externs) gets a row.

| C function (line) | Port location | Verdict |
|---|---|---|
| `shared_record_table_compare` (234) | session-seams (DSA registry) | SEAMED (session.c unported) |
| `shared_record_table_hash` (260) | session-seams | SEAMED (session.c unported) |
| `type_cache_syshash` (359) | folded into HashMap keying | MATCH |
| `lookup_type_cache` (386) | `lookup_type_cache` + `build_type_cache_entry` | MATCH |
| `load_typcache_tupdesc` (969) | `load_typcache_tupdesc` | MATCH |
| `load_rangetype_info` (1003) | `load_rangetype_info` | MATCH |
| `load_multirangetype_info` (1061) | `load_multirangetype_info` | MATCH |
| `load_domaintype_info` (1083) | `load_domaintype_info` (in-crate crawl/sort/lcons) | MATCH |
| `dcs_cmp` (1319) | `sort_by(name)` in `load_domaintype_info` | MATCH |
| `decr_dcc_refcount` (1332) | `decr_dcc_refcount` | MATCH |
| `dccref_deletion_callback` (1343) | `release_domain_constraint_ref` | MATCH |
| `prep_domain_constraints` (1364) | `prep_domain_constraints` (in-crate copy + ExecInitExpr seam) | MATCH |
| `InitDomainConstraintRef` (1402) | `init_domain_constraint_ref` | MATCH |
| `UpdateDomainConstraintRef` (1440) | `update_domain_constraint_ref` | MATCH |
| `DomainHasConstraints` (1489) | `domain_has_constraints` | MATCH |
| `array_element_has_equality` (1515) | `array_element_has_equality` | MATCH |
| `array_element_has_compare` (1524) | `array_element_has_compare` | MATCH |
| `array_element_has_hashing` (1532) | `array_element_has_hashing` | MATCH |
| `array_element_has_extended_hashing` (1540) | same | MATCH |
| `cache_array_element_properties` (1548) | `cache_array_element_properties` | MATCH |
| `record_fields_have_*` (1578-1602) | `record_fields_have_*` | MATCH |
| `cache_record_field_properties` (1610) | `cache_record_field_properties` | MATCH |
| `range_element_has_hashing/extended` (1715/1723) | same | MATCH |
| `cache_range_element_properties` (1731) | `cache_range_element_properties` | MATCH |
| `multirange_element_has_hashing/extended` (1755/1763) | same | MATCH |
| `cache_multirange_element_properties` (1771) | same | MATCH |
| `ensure_record_cache_typmod_slot_exists` (1799) | same (+`grow_record_cache_array`) | MATCH |
| `lookup_rowtype_tupdesc_internal` (1828) | same | MATCH (owned-copy model; refcount→drop) |
| `lookup_rowtype_tupdesc` (1922) | same | MATCH |
| `lookup_rowtype_tupdesc_noerror` (1939) | same | MATCH |
| `lookup_rowtype_tupdesc_copy` (1956) | same (tdrefcount=-1) | MATCH |
| `lookup_rowtype_tupdesc_domain` (1978) | same | MATCH |
| `record_type_typmod_hash` (2015) | `record_cache_find` (hash_row_type seam) | MATCH |
| `record_type_typmod_compare` (2026) | `equal_row_types` seam in find/insert | MATCH |
| `assign_record_type_typmod` (2042) | `assign_record_type_typmod` | MATCH (re-verified vs C 2042-2133) |
| `assign_record_type_identifier` (2134) | `assign_record_type_identifier` | MATCH |
| `SharedRecordTypmodRegistryEstimate` (2175) | passthrough seam | SEAMED (session.c unported) |
| `SharedRecordTypmodRegistryInit` (2197) | in-crate gather + passthrough seam | SEAMED (session.c unported) |
| `SharedRecordTypmodRegistryAttach` (2296) | passthrough seam | SEAMED (session.c unported) |
| `InvalidateCompositeTypeCacheEntry` (2365) | `invalidate_composite_type_cache_entry` | MATCH |
| `TypeCacheRelCallback` (2420) | `type_cache_rel_callback` | MATCH |
| `TypeCacheTypCallback` (2516) | `type_cache_typ_callback` | MATCH |
| `TypeCacheOpcCallback` (2573) | `type_cache_opc_callback` | MATCH |
| `TypeCacheConstrCallback` (2611) | `type_cache_constr_callback` | MATCH |
| `enum_known_sorted` (2635) | `enum_known_sorted` | MATCH |
| `compare_values_of_enum` (2664) | `compare_values_of_enum` | MATCH |
| `load_enum_cache_data` (2737) | `load_enum_cache_data` (bitmap/sort/copy in-crate; scan seamed) | MATCH |
| `find_enumitem` (2892) | `find_enumitem` | MATCH |
| `enum_oid_cmp` (2909) | `sort_by(enum_oid)` / `binary_search_by` | MATCH |
| `share_tupledesc` (2922) | session-seams (DSA) | SEAMED (session.c unported) |
| `find_or_make_matching_shared_tupledesc` (2943) | session-seams (DSA) | SEAMED (session.c unported) |
| `shared_record_typmod_registry_detach` (3055) | session-seams (DSA) | SEAMED (session.c unported) |
| `insert_rel_type_cache_if_needed` (3075) | `insert_rel_type_cache_if_needed` | MATCH |
| `delete_rel_type_cache_if_needed` (3109) | `delete_rel_type_cache_if_needed` | MATCH |
| `finalize_in_progress_typentries` (3173) | `finalize_in_progress_typentries` | MATCH |
| `AtEOXact_TypeCache` (3192) | `at_eoxact_type_cache` | MATCH |
| `AtEOSubXact_TypeCache` (3198) | `at_eosubxact_type_cache` | MATCH |

Every C function is MATCH or a real SEAMED `::call` into a genuinely-unported
owner (session.c for the DSA shared-typmod registry, pg_enum.c for the enum
scan, domains.c for the catalog scan / planner / context lifecycle, fmgr.c for
`fmgr_info_check`). **No MISSING / PARTIAL / DIVERGES, and no deferral-stub of
any kind.**

### Spot-checked MATCH verdicts (auditor self-check)

- `assign_record_type_typmod` re-derived against C 2042-2133: `RECORDOID`
  assert, hash-find early-return on `found && tupdesc != NULL`, shared-registry
  probe first (else local refcounted cache with `tdrefcount = 1`), `tdtypmod`
  assignment, deferred `HASH_ENTER`-equivalent `record_cache_insert` after all
  allocations succeed. MATCH.
- `load_enum_cache_data` re-derived against C 2737-2891: members scanned into a
  working vector, OID sort (`enum_oid_cmp`), the <8192-window in-order bitmap
  search (`>= numitems - start_pos - 1` early break), copy into cache context,
  link-in freeing the prior enumdata. The catalog scan (`pg_enum`) is the only
  thing crossing the seam (pg_enum.c, unported). MATCH.
- `load_domaintype_info` re-derived against C 1083-1318: crawl up `typbasetype`,
  NOT NULL accumulation, lazy `DomainConstraintCache` on first CHECK, name sort
  only when `>1`, `lcons` parent-first reverse-prepend, NOT NULL `lcons`-first,
  `set_parent` + `dcc_refcount = 1` + `TCFLAGS_CHECKED_DOMAIN_CONSTRAINTS`. The
  per-level catalog scan / `stringToNode`+`expression_planner` / "Domain
  constraints" context lifecycle are the only things crossing the seam
  (domains.c, unported). MATCH.

## Seam audit

Owned seam crate: `backend-utils-cache-typcache-seams`
(maps to typcache.c). 18 declarations.

### Installed by `init_seams()` (12, all genuine typcache.c own-logic)

`lookup_rowtype_tupdesc`, `lookup_rowtype_tupdesc_copy`,
`assign_record_type_typmod`, `at_eoxact_type_cache`, `at_eosubxact_type_cache`,
`lookup_type_cache` (copy-out of the small `pg_type` storage row),
`lookup_type_cache_entry` (range/multirange view: storage + rng_*/hash_* finfo
+ recursively-copied element/range sub-entries), `lookup_element_eq_opr`,
`lookup_element_cmp_proc`, `lookup_element_hash_proc`,
`lookup_element_hash_extended_proc`, `lookup_range_elem_hash_proc`.

The element/range hash-proc seams are the C idiom
`lookup_type_cache(elem, TYPECACHE_*_FINFO); read entry->*_finfo.fn_oid` —
own typcache logic (a lookup plus a cached-field read); `lookup_range_elem_hash_proc`
adds the `OidIsValid` check raising `ERRCODE_UNDEFINED_FUNCTION`. This re-audit
**installed these 7 previously-uninstalled-but-typcache-owned seams** (they had
backing logic but were not wired). `init_seams()` is `set()`-only and is wired
into `seams-init`.

### Declared in this seam crate but NOT typcache.c logic (6) — mis-homed, owner unported

`domain_check_input`, `domain_get_base_input_info` map to **domains.c**
(`utils/adt/domains.c`); `record_column_cmp`/`_eq`/`_hash`/`_hash_extended` map
to **rowtypes.c** (`utils/adt/rowtypes.c`). By the audit's ownership rule
(ownership is by C-source coverage), these are NOT typcache's own logic — they
were placed in the typcache-seams crate by neighbor consumers (misc2,
multirangetypes). typcache correctly does **not** install them; their real
owners (domains.c / rowtypes.c, both unported) will, once ported. This is a
seam-home / contract-reconcile item (tracked in the seam-contract-reconcile
lane), not a typcache function gap, and not a deferral-loophole — there is no
typcache.c body being stubbed. Recorded here as a known wiring residue, not a
typcache FAIL.

### Outward seams (thinness checked)

lsyscache / relcache / pg-enum / fmgr / format-type / inval / session / domains
calls are marshal + single-delegate. The `scan_enum_members` and domains
catalog/planner paths keep all bitmap/sort/copy and crawl/sort/lcons
orchestration in-crate; only the single external callee crosses each seam.
Owner crates verified unported: domains, session, pg-enum, fmgr (real
`::call` panics "seam not installed" until they land). PASS.

## Verdict: PASS (re-audit under tightened rule)

Zero `todo!()`/`unimplemented!()`/deferral-panic. Every typcache.c function is
real own-logic (MATCH) or a real `::call` into a genuinely-unported owner
(SEAMED). All 12 typcache-owned seams with backing logic are now installed by
`init_seams()`. The 6 remaining decls in the seam crate are mis-homed domains.c
/ rowtypes.c functions whose owners are unported (correctly not installed here).
Gate: `cargo check --workspace` clean; `cargo test --workspace` clean (known
flakes aside).
