# Audit: backend-catalog-pg-constraint

Independent function-by-function logic audit against
`src/backend/catalog/pg_constraint.c` (PostgreSQL 18.3, 1776 lines).
Re-derived from C ground truth + header constants; the port's comments were not
trusted.

- C: `/Users/malisper/workspace/work/pgrust/postgres-18.3/src/backend/catalog/pg_constraint.c`
- Port: `/private/tmp/pg-constraint-land-wt/crates/backend-catalog-pg-constraint/src/lib.rs`
- Added types: `crates/types-catalog/src/pg_constraint.rs`
- Inward seam crate: `crates/backend-catalog-pg-constraint-seams/src/lib.rs`
- New seam crate: `crates/backend-commands-indexcmds-seams/src/lib.rs`

## STEP 1 — Inventory

The C file defines exactly 22 functions, all public (no statics). All 22 are
present in the port. None missing.

## STEP 2 — Per-function verdicts

| # | C function | C lines | Port location | Verdict |
|---|---|---|---|---|
| 1 | CreateConstraintEntry | 50-397 | lib.rs:290-578 | MATCH |
| 2 | ConstraintNameIsUsed | 411-447 | lib.rs:586-623 | MATCH |
| 3 | ConstraintNameExists | 456-485 | lib.rs:630-649 | MATCH |
| 4 | ChooseConstraintName | 512-580 | lib.rs:657-715 | MATCH |
| 5 | findNotNullConstraintAttnum | 591-633 | lib.rs:724-756 | MATCH |
| 6 | findNotNullConstraint | 641-651 | lib.rs:763-774 | MATCH |
| 7 | findDomainNotNullConstraint | 657-695 | lib.rs:781-807 | MATCH |
| 8 | extractNotNullColumn | 701-722 | lib.rs:815-826 | MATCH |
| 9 | AdjustNotNullInheritance | 741-821 | lib.rs:834-949 | MATCH |
| 10 | RelationGetNotNullConstraints | 833-905 | lib.rs:981-1017 | PARTIAL (keystone-blocked; acceptable — see below) |
| 11 | RemoveConstraintById | 911-990 | lib.rs:1024-1098 | MATCH |
| 12 | RenameConstraintById | 1002-1045 | lib.rs:1106-1166 | MATCH |
| 13 | AlterConstraintNamespaces | 1054-1112 | lib.rs:1174-1234 | MATCH |
| 14 | ConstraintSetParentConstraint | 1123-1189 | lib.rs:1242-1344 | MATCH |
| 15 | get_relation_constraint_oid | 1197-1240 | lib.rs:1351-1390 | MATCH |
| 16 | get_relation_constraint_attnos | 1254-1333 | lib.rs:1399-1461 | MATCH |
| 17 | get_relation_idx_constraint_oid | 1343-1383 | lib.rs:1469-1497 | MATCH |
| 18 | get_domain_constraint_oid | 1390-1433 | lib.rs:1504-1543 | MATCH |
| 19 | get_primary_key_attnos | 1449-1526 | lib.rs:1553-1623 | MATCH |
| 20 | DeconstructFkConstraintRow | 1535-1650 | lib.rs:1648-1729 | MATCH |
| 21 | FindFKPeriodOpers | 1665-1722 | lib.rs:1746-1798 | MATCH |
| 22 | check_functional_grouping | 1739-1776 | lib.rs:1807-1845 | MATCH |

### Notes per function

1. **CreateConstraintEntry** — full dependency-record order/dedup preserved:
   auto-deps (rel-or-columns AUTO via `constraintNTotalKeys>0`; domain AUTO),
   normal-deps (foreign rel-or-columns via `foreignNKeys>0`; FK supporting index
   only when `constraintType==CONSTRAINT_FOREIGN`; the pf/pp/ff operator deps
   with `pp!=pf`/`ff!=pf` dedup), conExpr CHECK dep via
   `record_dependency_on_single_rel_expr`, then
   `invoke_object_post_create_hook_arg(...,is_internal)`. The two `Assert`s
   become `debug_assert!`. Array construction + insert are seamed to the indexing
   owner (`catalog_tuple_insert_pg_constraint`) carrying every column. The `Max`
   sizing local is computed but unused (`_nkeys`), matching the C's shared
   `fkdatums` buffer which has no observable effect. MATCH.
2. **ConstraintNameIsUsed** — 3 scankeys (conrelid=objId if Relation else
   Invalid; contypid=objId if Domain else Invalid; conname), early-stop on first
   match. MATCH.
3. **ConstraintNameExists** — 2 scankeys (conname, connamespace) on
   ConstraintNameNspIndexId. MATCH.
4. **ChooseConstraintName** — pass counter, empty-label `++pass` first iteration,
   `others` strcmp loop, catalog scan on conflict, increment-and-retry. Truncation
   to NAMEDATALEN-1 via `truncate_namedatalen`. `makeObjectName` seamed. MATCH.
5/6/7. find* — correct contype/convalidated filters, AccessShareLock, copy-and-break.
   findNotNullConstraint guards `attnum <= InvalidAttrNumber` (InvalidAttrNumber=0).
   MATCH.
8. **extractNotNullColumn** — checks `ndim!=1 || hasnull || elemtype!=INT2OID ||
   dim0!=1` (note: requires dim0==1, no `numcols<0` here), error
   `"conkey is not a 1-D smallint array"`, returns `data[0]`. MATCH.
9. **AdjustNotNullInheritance** — all 3 ereport guards verbatim (NO INHERIT
   mismatch / NOT VALID / name mismatch), each with correct SQLSTATE
   `ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE`, hint/detail strings preserved
   (hint text inlined including the `%s` action verbatim). `!is_local` →
   coninhcount++ with overflow→`too many inheritance parents`
   (ERRCODE_PROGRAM_LIMIT_EXCEEDED); else `!conislocal` → flip true. `changed`
   gate on the CatalogTupleUpdate. MATCH.
10. **RelationGetNotNullConstraints** — see design conformance below. PARTIAL,
    judged acceptable.
11. **RemoveConstraintById** — `cache lookup failed for constraint %u` on miss;
    conrelid branch opens AccessExclusiveLock, CHECK→read relchecks, `==0` guard
    `relation "%s" has relchecks = 0`, decrement; contypid no-op; else
    `constraint %u is not of a known type`; CatalogTupleDelete by tid; rel closed
    NoLock (lock kept to xact end). MATCH.
12. **RenameConstraintById** — duplicate-name checks for relation
    (`constraint "%s" for relation "%s" already exists`) and domain
    (`constraint "%s" for domain %s already exists` via format_type_be), both
    ERRCODE_DUPLICATE_OBJECT; namestrcpy rename; post-alter hook. MATCH.
13. **AlterConstraintNamespaces** — 2 scankeys (conrelid/contypid by isType),
    object_address_present skip, `connamespace==oldNspId && oldNspId!=newNspId`
    update guard, post-alter hook, add_exact_object_address. MATCH.
14. **ConstraintSetParentConstraint** — valid-parent: Assert coninhcount==0,
    `constraint %u already has a parent constraint` if conparentid set,
    conislocal=false, coninhcount++ overflow guard, conparentid set, update, two
    recordDependencyOn (PARTITION_PRI on constraint, PARTITION_SEC on relation).
    Invalid-parent: coninhcount--, conislocal=true, conparentid=Invalid, Assert,
    update, two deleteDependencyRecordsForClass. MATCH.
15/18. get_*_constraint_oid — 3 scankeys, single-row, missing_ok→error
    `constraint "%s" for table/domain "%s/%s" does not exist`
    (ERRCODE_UNDEFINED_OBJECT). MATCH.
16. **get_relation_constraint_attnos** — uses `heap_get_conkey` (heap_getattr
    equivalent, NULL→skip via `Option::None`, no error in the null case, matching
    C which only fills the bitmap when `!isNull`); validation
    `ndim!=1 || numcols<0 || hasnull || elemtype!=INT2OID`; bms offset
    `attnums[i] - FirstLowInvalidHeapAttributeNumber`. MATCH.
17. **get_relation_idx_constraint_oid** — contype in {PRIMARY,UNIQUE,EXCLUSION}
    filter, conindid==indexId break. MATCH.
19. **get_primary_key_attnos** — contype!=PRIMARY skip; `condeferrable &&
    !deferrableOk` → break (whole scan); null-conkey →
    `null conkey for constraint %u`; validation as C; bms offset; sets
    constraintOid then break. MATCH.
20. **DeconstructFkConstraintRow** — conkey ndim/hasnull/elemtype check then
    `numkeys<=0 || numkeys>INDEX_MAX_KEYS` → `foreign key constraint cannot have
    %d columns`; confkey requires `dim0==numkeys`; pf/pp/ff guarded by want_*
    (= C's NULL-pointer skip), each requiring `dim0==numkeys` + OIDOID, errors
    `conpfeqop/conppeqop/conffeqop is not a 1-D Oid array`; confdelsetcols
    SQL-NULL → num=0, else INT2OID check + `confdelsetcols is not a 1-D smallint
    array`; `numfks=numkeys`. MATCH. (See design note on want_del_set_cols None.)
21. **FindFKPeriodOpers** — opclass lookup → on miss `cache lookup failed for
    opclass %u`; opcintype must be ANYRANGEOID/ANYMULTIRANGEOID else
    `invalid type for PERIOD part of foreign key` (FEATURE_NOT_SUPPORTED) +
    detail `Only range and multirange are supported.`; two
    get_operator_from_compare_type (InvalidOid rhs, then ANYMULTIRANGEOID rhs,
    both COMPARE_CONTAINED_BY); intersect op by opcintype with default
    `unexpected opcintype: %u`. MATCH.
22. **check_functional_grouping** — no PK → false; collect Var attnos matching
    varno/varlevelsup (IsA(Var) via `Node::Expr(Expr::Var)`); bms_is_subset →
    append constraintOid + true. MATCH.

## STEP 3 — Seam / wiring audit

**Inward seams (owned crate `backend-catalog-pg-constraint-seams`):**
init_seams() (lib.rs:1980-1988) installs all 5 declared seams
(register_constraint_inval_callback, load_fk_constraint, constraint_hash_value,
get_ri_constraint_root, find_fk_period_opers). Wired into
`crates/seams-init/src/lib.rs:49`. `cargo test -p seams-init` PASSES both
`every_declared_seam_is_installed_by_its_owner` and
`every_seam_installing_crate_is_wired_into_init_all`.

Seam-body faithfulness:
- **find_fk_period_opers** = get_index_column_opclass(conindid,nkeys) + the real
  in-crate FindFKPeriodOpers. Faithful.
- **get_ri_constraint_root** = conparentid walk via SearchSysCache1(CONSTROID),
  `cache lookup failed for constraint %u` on miss, returns when conparentid
  invalid. Matches ri_triggers.c get_ri_constraint_root. Faithful.
- **load_fk_constraint** = SearchSysCache1(CONSTROID) tuple+form, contype==FOREIGN
  check with `constraint %u is not a foreign key constraint`, oid hash value, the
  real in-crate DeconstructFkConstraintRow(want all 4 groups), field mapping
  confrelid→pk_relid / conrelid→fk_relid / confupdtype/confdeltype/confmatchtype
  /conperiod→hasperiod/conparentid/conindid. Does NOT compute root/root-hash or
  period opers (left to the ri cache.rs caller, as required). Faithful.
- **constraint_hash_value** = GetSysCacheHashValue1(CONSTROID). Faithful.
- **register_constraint_inval_callback** = register_syscache_callback(CONSTROID,
  adapter→ri invalidate_constraint_cache_callback) ignoring cacheid. Faithful.

These bodies legitimately live in this crate (it owns pg_constraint access); the
projection logic (DeconstructFkConstraintRow) is real in-crate code, not pushed
into another crate. Acceptable.

**Outward panic-seams** (catalog_tuple_insert/update/delete_pg_constraint,
search_constraint_form/tuple_by_oid, get_conkey_array, deconstruct_fk_arrays,
heap_get_conkey, fetch/decrement_relchecks, get_syscache_hash_value_constroid,
make_object_name, get_operator_from_compare_type, the objectaddress/dependency
helpers): each call site is a thin marshal+delegate. The validation, loops, and
error strings that wrap them all remain in-crate. The seam contracts correctly
distinguish `SysCacheGetAttrNotNull` (get_conkey_array / deconstruct_fk_arrays)
from `heap_getattr` (heap_get_conkey, NULL-aware via Option). No real
pg_constraint.c logic was found inside another crate's seam body. No findings.

## STEP 3b — Design conformance

- **ObjectAddressesHandle** (AlterConstraintNamespaces `objsMoved`): inherited
  opacity — it is the runtime `ObjectAddresses *` owned by the objectaddress
  unit, threaded through unchanged. Not an invented model. OK.
- Allocating fns take `Mcx` and return `PgResult`; OOM via try_reserve + mcx.oom.
  OK.
- No shared statics for per-backend globals. OK.
- Lock spans: every table_open is matched by a `.close(lock)?` on the success
  path, and any intermediate `?`/early-Err returns before the close abandon the
  guard which (per repo convention, `con_ctx`/scope MemoryContext + RelationData
  guard) drops/closes on unwind. The C keeps the constraint rel lock to xact end
  in RemoveConstraintById (closes the *target* rel NoLock); port mirrors
  (`rel.close(NoLock)`). No lock leaked across a `?` without a guard. OK.
- No divergence markers, no todo!()/unimplemented!() in the crate.

**RelationGetNotNullConstraints (PARTIAL, judged ACCEPTABLE):** the port returns
`Vec<NotNullConstraint>` instead of building `CookedConstraint`/`Constraint`
parse nodes, and ignores `cooked`. Those node types are not modeled anywhere in
this tree (unbuilt keystone), and there are no consumers yet. The returned struct
preserves every conForm field the C reads (oid, conname, attnum, convalidated,
connoinherit). The C raw-`Constraint` branch additionally resolves
`get_attname(relid, colnum)` for `Constraint.keys`; the port preserves `attnum`
(colnum) instead, from which the name is recoverable losslessly by any future
node-builder. This is keystone-deferred, not own-logic loss; consistent with the
repo's "mirror-and-defer to keystone" policy. Acceptable, recorded as PARTIAL for
visibility.

## Constants verified against the headers (not from memory)

| Constant | C / header value | Port value | OK |
|---|---|---|---|
| ConstraintOidIndexId | 2667 (pg_constraint.h:182) | 2667 | yes |
| ConstraintRelidTypidNameIndexId | **2665** (pg_constraint.h:180) | 2665 | yes (prompt's "2625" was wrong; port correct) |
| ConstraintNameNspIndexId | 2664 (pg_constraint.h:179) | 2664 | yes |
| CONSTRAINT_CHECK/FOREIGN/NOTNULL/PRIMARY/UNIQUE/EXCLUSION | c/f/n/p/u/x | same | yes |
| Anum_pg_constraint_* (1..28) | field order | 1..28 | yes |
| Natts_pg_constraint | 28 | 28 | yes |
| FirstLowInvalidHeapAttributeNumber | **-7** (access/sysattr.h:27) | -7 | yes (prompt's "-8" was wrong; port correct) |
| INDEX_MAX_KEYS | 32 (pg_config_manual.h:69) | 32 | yes |
| F_NAMEEQ / F_OIDEQ | 62 / 184 | 62 / 184 | yes |
| INT2OID / OIDOID | 21 / 26 | 21 / 26 | yes |
| ANYRANGEOID / ANYMULTIRANGEOID | 3831 / 4537 (pg_type.dat) | 3831 / 4537 | yes |
| OID_RANGE_INTERSECT_RANGE_OP | 3900 (pg_operator.dat) | 3900 | yes |
| OID_MULTIRANGE_INTERSECT_MULTIRANGE_OP | 4394 (pg_operator.dat) | 4394 | yes |
| CONSTROID syscache id | 19 | 19 | yes |
| COMPARE_CONTAINED_BY | enum member (access/cmptype.h) | COMPARE_CONTAINED_BY | yes |

No wrong constant values found. (Two values in the prompt's "verify!" list —
ConstraintRelidTypidNameIndexId=2625 and FirstLowInvalidHeapAttributeNumber=-8 —
were themselves incorrect; the port matches the real headers, 2665 and -7.)

## Minor non-blocking observations (no behavioral effect)

- `types-catalog` `ConstraintCategory::Type` carries a doc comment referencing
  `CONSTRAINT_NOTNULL`; the actual C third enum member is `CONSTRAINT_ASSERTION`
  (pg_constraint.h:217, "for future expansion"). Doc-string inaccuracy only;
  pg_constraint.c never compares against the third member, so no logic impact.
- `DeconstructFkConstraintRow` with `want_del_set_cols=true` and a SQL-NULL
  confdelsetcols sets `fk_del_set_cols = Some(Vec::new())` + `num=0`. The C leaves
  `*fk_del_set_cols` untouched and sets `*num_fk_del_set_cols = 0`; callers gate on
  the count, so the empty vec is behaviorally equivalent. No finding.

## Spot-checked MATCH verdicts (deep)

1. **CreateConstraintEntry dependency order/dedup** — verified line-by-line: auto
   then normal then conExpr then hook; `pp!=pf`/`ff!=pf` operator dedup; subId =
   constraintKey[i]/foreignKey[i]; FK-index dep gated on contype==FOREIGN. Exact.
2. **get_primary_key_attnos deferrable break** — `condeferrable && !deferrableOk`
   returns Ok(false) leaving pkattnos=None/Invalid (C `break` before any set);
   null-conkey uses con.oid. Exact (scan_err re-raise is equivalent since the
   closure returns Ok(false) and stops the scan immediately).
3. **ConstraintSetParentConstraint else-branch** — coninhcount--/conislocal=true/
   conparentid=Invalid, update, two deleteDependencyRecordsForClass (PARTITION_PRI
   on ConstraintRelationId, PARTITION_SEC on RelationRelationId). Exact.

## OVERALL: PASS

All 22 functions are MATCH except RelationGetNotNullConstraints (PARTIAL,
keystone-deferred and judged acceptable — lossless field preservation, no
consumers, no own-logic loss). Zero seam findings: the 5 inward seams are
installed and wired, their bodies faithful; outward panic-seams are thin
delegations with all validation/loops/error strings kept in-crate. All constants
verified correct against the headers. `cargo test -p seams-init` and
`cargo test -p backend-catalog-pg-constraint` both pass.
