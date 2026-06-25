# Audit: backend-access-hashvalidate

Unit: `backend-access-hashvalidate` (`src/backend/access/hash/hashvalidate.c`,
351 lines, PostgreSQL 18.3).
Crates audited: `crates/backend-access-hashvalidate`, plus the new seam crates
it introduced (`backend-access-index-amvalidate-seams`,
`backend-utils-cache-syscache-seams`, `backend-utils-cache-lsyscache-seams`,
`backend-utils-adt-regproc-seams`, `backend-utils-adt-format-type-seams`,
`backend-utils-error-seams`, `backend-access-transam-xact-seams`) and the new
types crates (`types-error`, `types-hash`, `types-amvalidate`).
Cross-checked against
`../pgrust/c2rust-runs/backend-access-hashvalidate/src/hashvalidate.rs`.
Auditor: independent re-derivation from the C sources and headers
(`access/hash.h`, `access/stratnum.h`, `access/amapi.h`, `access/amvalidate.h`,
`catalog/pg_amop.h`, `catalog/pg_type.dat`, `catalog/pg_am.dat`,
`utils/elog.h`, `utils/errcodes.h`).

## Function inventory (every definition in hashvalidate.c)

hashvalidate.c defines exactly two functions. The c2rust rendering additionally
materializes three post-preprocessor header inlines (`ObjectIdGetDatum`,
`list_length`, `GETSTRUCT`); in the owned model these are representation shims
with no separate logic (Datum boxing, `Vec::len`, struct projection) and are
absorbed into the row types — noted below, not separate port functions.

| # | C function (hashvalidate.c) | Port location | Verdict | Notes |
|---|---|---|---|---|
| 1 | `hashvalidate` (:39) | `lib.rs::hashvalidate` | MATCH | Re-derived branch-by-branch against the C and the c2rust rendering. CLAOID lookup via `search_opclass` seam; `!HeapTupleIsValid` → `elog(ERROR, "cache lookup failed for operator class %u")` carried as `Err(PgError::error(...))` — level ERROR(21), default SQLSTATE XX000, exactly elog.c's `errmsg_internal` default (c2rust:1039-1068 confirms level 21, no errcode call). `get_opfamily_name(opfamilyoid, false)` seamed. Proc loop: lefttype≠righttype INFO report; `switch (amprocnum)` with arms 1 (`check_amproc_signature(amproc, INT4OID, true, 1, 1, lefttype)`), 2 (`(amproc, INT8OID, true, 2, 2, lefttype, INT8OID)`), 3 (`check_amoptsproc_signature`), default → INFO invalid-support-number + `result = false` + `continue` (don't want additional message — c2rust `current_block_46` skip confirmed); `!ok` → INFO wrong-signature; `ok && (num==1\|\|2)` → `list_append_unique_oid(hashabletypes, lefttype)`. Opr loop: `amopstrategy < 1 \|\| > HTMaxStrategyNumber(1)` INFO; `amoppurpose != AMOP_SEARCH('s') \|\| OidIsValid(amopsortfamily)` INFO (ORDER BY); `!check_amop_signature(amopopr, BOOLOID, left, right)` INFO; `!list_member_oid` for either side INFO. `identify_opfamily_groups` seamed (rows projected in-crate to the fields amvalidate.c:65-112 actually reads: amoplefttype/amoprighttype/amopstrategy, amproclefttype/amprocrighttype/amprocnum — verified against amvalidate.c). Group loop: last group with `lefttype == righttype == opcintype` remembered (port overwrites like C); `operatorset != (1 << HTEqualStrategyNumber)` = `!= 2` INFO (port `1u64 << 1`, same value vs `uint64 operatorset`); `opclassgroup == NULL` INFO using `opclassname`; `list_length(grouplist) != hashable²` INFO. All seven INFO reports: level INFO(17), SQLSTATE 42P17, message strings byte-compared against the c2rust string literals with `%s`→"hash"/seamed-formatter substitutions. Releases = owned drops. Returns accumulated `result`. |
| 2 | `hashadjustmembers` (:262) | `lib.rs::hashadjustmembers` | MATCH | Re-derived against c2rust:1638-1695. `OidIsValid(opclassoid)` → `CommandCounterIncrement()` (seam) then `get_opclass_input_type` (seam), else `opcintype = InvalidOid`. `foreach` over `list_concat_copy(operators, functions)` — in C the copied list shares the `OpFamilyMember*` pointers, so mutations hit the originals; the port chains `iter_mut()` over both slices in the same order, equivalent. Per member, the three-way classification matches exactly: (a) `is_func && number != HASHSTANDARD_PROC` → soft family dep; (b) `lefttype != righttype` → soft family dep; (c) else: if `lefttype != opcintype` refresh `opcintype = lefttype` and `opclassoid = opclass_for_family_datatype(HASH_AM_OID=405, opfamilyoid, opcintype)` (memoized-even-on-failure, as the C comment demands — the port also reuses the stale `opclassoid` when `lefttype == opcintype`, matching C), then `OidIsValid(opclassoid)` → hard opclass dep else soft family dep. Field writes (`ref_is_hard`, `ref_is_family`, `refobjid`) identical in all five outcomes. |
| — | `ObjectIdGetDatum` / `list_length` / `GETSTRUCT` (header inlines, c2rust only) | absorbed | MATCH (subsumed) | Datum boxing, list length, tuple-struct projection — identity operations in the owned row model (seam crates pass `Oid` directly; `Vec::len`; projected row structs). |

## Constants (verified against headers, not memory)

| Constant | Header | Value | Port |
|---|---|---|---|
| `HASHSTANDARD_PROC` | hash.h:355 | 1 | types-hash `1` ✓ |
| `HASHEXTENDED_PROC` | hash.h:356 | 2 | types-hash `2` ✓ |
| `HASHOPTIONS_PROC` | hash.h:357 | 3 | types-hash `3` ✓ |
| `HASHNProcs` | hash.h:358 | 3 | types-hash `3` ✓ (unused by this unit) |
| `HTEqualStrategyNumber` | stratnum.h:41 | 1 | types-hash `1` ✓ |
| `HTMaxStrategyNumber` | stratnum.h:43 | 1 | types-hash `1` ✓ |
| `AMOP_SEARCH` | pg_amop.h:100 | `'s'` | crate `b's' as i8` ✓ |
| `BOOLOID` | pg_type.dat (oid 16) | 16 | crate `16` ✓ |
| `INT8OID` | pg_type.dat (oid 20) | 20 | crate `20` ✓ |
| `INT4OID` | pg_type.dat (oid 23) | 23 | crate `23` ✓ |
| `HASH_AM_OID` | pg_am.dat:21 (oid 405) | 405 | crate `405` ✓ |
| `INFO` | elog.h | 17 | types-error `ErrorLevel(17)` ✓ (c2rust emits literal 17) |
| `ERROR` | elog.h | 21 | types-error `ErrorLevel(21)` ✓ (c2rust emits literal 21) |
| `ERRCODE_INVALID_OBJECT_DEFINITION` | errcodes.h | `MAKE_SQLSTATE('4','2','P','1','7')` | types-error `make_sqlstate(*b"42P17")`; the `pg_sixbit` packing reproduces elog.h's `PGSIXBIT` shift sum bit-for-bit (cross-checked against the c2rust inline expansion); unit test asserts it ✓ |

Width notes (checked deliberately): `amprocnum`/`amopstrategy` are `int16`
catalog columns — port rows use `i16`; comparisons `n as u16 == K` (K ∈ 1..3)
are equality-faithful for every `i16` value, identical to C's `(int)` widening
compare. `OpFamilyMember.number` is C `int`; the port mirror uses `i16` — every
value reaching `amadjustmembers` is a validated strategy/support number (an
`int16` catalog quantity), so behavior is identical on every representable
input. `amoppurpose` is C `char` → `i8`. `OpFamilyOpFuncGroup.operatorset/
functionset` are `uint64` → `u64`. `1u64 << HTEqualStrategyNumber` = 2 matches
C's int-shift-then-uint64-compare. `grouplist.len() != hashable²` in `usize`
matches the C `int` arithmetic for all feasible catalog sizes.

## Seam audit

Inward: none. The crate's `init_seams()` is an empty hook (no inward seam
declarations exist for this unit), and `seams-init::init_all()` calls it —
wiring is uniform and complete. No `set()` calls exist outside `seam-core`'s
own doctest and this crate's `#[cfg(test)]` stubs (test-only, acceptable).

Outward (all on unported owner units — a direct dependency cannot exist yet;
each call site is thin marshal + one delegate + result conversion, no logic in
any seam path):

| Seam crate | Declarations | C owner | Assessment |
|---|---|---|---|
| `backend-access-index-amvalidate-seams` | `check_amproc_signature` (variadic→slice), `check_amoptsproc_signature`, `check_amop_signature`, `opclass_for_family_datatype`, `identify_opfamily_groups` | amvalidate.c | Clean. The variadic→slice conversion is a faithful signature translation (C passes exactly `maxargs` OIDs). The row projection feeding `identify_opfamily_groups` happens in the crate body, projecting exactly the six fields amvalidate.c reads; the seam itself is declaration-only. |
| `backend-utils-cache-syscache-seams` | `search_opclass` (CLAOID), `search_amop_list` (AMOPSTRATEGY), `search_amproc_list` (AMPROCNUM) | syscache.c | Clean. Cache miss = `Ok(None)`; the caller raises `cache lookup failed` itself, as in C. Projected rows carry every field hashvalidate reads (opcfamily/opcintype/opcname; all six pg_amop fields used; all four pg_amproc fields used). |
| `backend-utils-cache-lsyscache-seams` | `get_opfamily_name`, `get_opclass_input_type` | lsyscache.c | Clean; `missing_ok` semantics documented to match C (false → raise). |
| `backend-utils-adt-regproc-seams` | `format_procedure`, `format_operator` | regproc.c | Clean. |
| `backend-utils-adt-format-type-seams` | `format_type_be` | format_type.c | Clean. |
| `backend-utils-error-seams` | `ereport` | elog.c | Clean; contract states `< ERROR` returns `Ok(())` (C `errfinish` returns at INFO) — the validator's `report_info` relies on exactly that, matching C's non-raising INFO. |
| `backend-access-transam-xact-seams` | `command_counter_increment` | xact.c | Clean. |

All seam crates are pure `seam_core::seam!` declarations — zero branching,
construction, or computation. Until the owners land, calls panic loudly
(unported-callee panic, which is acceptable); no logic of this unit's two
functions was displaced behind a seam.

## Build / tests

`cargo build --workspace` clean; `cargo test -p backend-access-hashvalidate`:
12/12 pass (both classification paths of `hashadjustmembers` memoization, all
hashvalidate scenario stubs, the 42P17 packing assertion, and the
cache-lookup-failed error message).

## Spot-check of the audit itself

`hashadjustmembers` was re-derived a second time directly against the c2rust
control flow (lines 1638–1695), confirming the memoized-lookup branch and all
five write outcomes; the `default:`-arm `continue` in `hashvalidate` was
confirmed against c2rust's `current_block_46` dispatch (the wrong-signature and
hashable-types blocks are skipped exactly when the invalid-support-number arm
ran).

## Verdict

**PASS** — both functions MATCH (outward calls SEAMED per the rules), all
constants verified against headers, zero seam findings, fix rounds: 0.
