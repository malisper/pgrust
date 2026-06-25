# Audit: backend-catalog-heap — constraint-cooker / attribute-mutate half

Scope: the functions newly landed in `src/constraints.rs` and the
`CopyStatistics` addition in `src/statistics.rs`. The relation create/delete/
drop core was audited previously; not re-covered here.

C source: `../pgrust/postgres-18.3/src/backend/catalog/heap.c`.

## Per-function table

| C function (heap.c line) | Port | Verdict | Notes |
|---|---|---|---|
| `cookDefault` (3323) | constraints.rs `cookDefault` | MATCH | EXPR_KIND select on `attgenerated`; nested-generated + mutable + virtual-security checks gated on `attgenerated`; `coerce_to_target_type` with COERCION_ASSIGNMENT/COERCE_IMPLICIT_CAST/-1 then DATATYPE_MISMATCH on None; `assign_expr_collations`. Returns `Option<Expr>` (None = C `expr == NULL`). The `contain_var_clause` assert is a debug_assert. |
| `cookConstraint` (3404) | constraints.rs `cookConstraint` | MATCH | transformExpr(EXPR_KIND_CHECK_CONSTRAINT) → coerce_to_boolean("CHECK") → assign_expr_collations → `p_rtable.len() != 1` ⇒ INVALID_COLUMN_REFERENCE. |
| `check_nested_generated_walker` (3183) | constraints.rs | MATCH | Var arm: rt_fetch(varno) via `p_rtable[varno-1].relid`; invalid relid ⇒ false; `attnum>0 && get_attgenerated` ⇒ INVALID_OBJECT_DEFINITION w/ get_attname; whole-row (attnum==0) ⇒ INVALID_OBJECT_DEFINITION. ereport carried out-of-band via `RefCell<Option<PgError>>` (established repo pattern for bool walkers). |
| `check_nested_generated` (3225) | constraints.rs | MATCH | thin driver. |
| `contains_user_functions_checker` (3253) | constraints.rs | MATCH | `func_id >= FirstUnpinnedObjectId` (12000, verified vs transam.h). |
| `check_virtual_generated_security_walker` (3268) | constraints.rs | MATCH | non-List guard; check_functions_in_node ⇒ FEATURE_NOT_SUPPORTED w/ exprLocation+parser_errposition; `exprType >= FirstUnpinnedObjectId` ⇒ FEATURE_NOT_SUPPORTED. |
| `check_virtual_generated_security` (3305) | constraints.rs | MATCH | thin driver. |
| `StoreRelCheck` (2147) | constraints.rs `StoreRelCheck` | MATCH | nodeToString → ccbin; pull_var_clause + varattno dedup (keycount=deduped len); partitioned NO INHERIT ⇒ INVALID_TABLE_DEFINITION; CreateConstraintEntry CONSTRAINT_CHECK with the full C argument vector (verified positionally vs the C call). |
| `StoreRelNotNull` (2254) | constraints.rs `StoreRelNotNull` | MATCH | CreateConstraintEntry CONSTRAINT_NOTNULL, single-attr key, isEnforced=true. |
| `StoreConstraints` (2310) | constraints.rs `StoreConstraints` | MATCH (currently NIL-only at all call sites) | NIL ⇒ return; CCI; DEFAULT→StoreAttrDefault, CHECK→StoreRelCheck + numchecks; SetRelationNumChecks if numchecks>0. Cooked fields ride the Node::Constraint carrier (attnum=location, is_local=initially_valid) exactly as `make_cooked_node` encodes them; `inhcount` is not round-tripped (the carrier has no slot and no consumer reads it) — the only field-level limitation, on a branch no current caller exercises (heap_create_with_catalog passes NIL). |
| `AddRelationNewConstraints` (2385) | constraints.rs `AddRelationNewConstraints` | MATCH | numoldchecks from rd_att.constr.num_check; make_parsestate+addRangeTableEntryForRelation(AccessShareLock)+addNSItemToQuery; DEFAULT loop with NULL-const skip (`expr==NULL || (!generated && Const.constisnull)`); CHECK loop (cookConstraint / cooked-string branch seam-panics — `stringToNode` unported; named-constraint dup check + MergeWithExistingConstraint; unnamed → pull_var_clause+dedup+ChooseConstraintName); NOTNULL loop (get_attnum bounds, AdjustNotNullInheritance, ConstraintNameIsUsed, ChooseConstraintName); SetRelationNumChecks at end. The `generated` triple value is used for both the EXPR_KIND select and the null-skip, which is faithful because tablecmds sets `colDef.generated == atp->attgenerated` (verified at create.rs:309/538). |
| `MergeWithExistingConstraint` (2712) | constraints.rs `MergeWithExistingConstraint` | SEAMED (mirror-and-panic) | Needs a `conbin` reader (fastgetattr+stringToNode+equal) + an extended pg_constraint field-update carrier the typed model has not assembled. The real `nodeToString(expr)` flatten is done in-crate; the lookup/conflict/update is delegated to `merge_with_existing_constraint` (uninstalled ⇒ loud panic). |
| `AddRelationNotNullConstraints` (2897) | constraints.rs `AddRelationNotNullConstraints` | MATCH | Two index-based outer loops with inner deletion mirroring the C (no foreach to preserve the dual-loop deletion semantics); givennames/nnnames tracking; conflicting-NO-INHERIT (SYNTAX_ERROR), conflicting-names (SYNTAX_ERROR), NO-INHERIT-vs-inherited (DATATYPE_MISMATCH), dup-given-name (DUPLICATE_OBJECT); inherited-leftover loop with inhcount counting + name reuse. |
| `SetRelationNumChecks` (3149) | constraints.rs `SetRelationNumChecks` | PARTIAL→SEAMED on store branch (per task) | The relchecks read (`fetch_relchecks`) and the `relchecks == numchecks` ⇒ `CacheInvalidateRelcache` branch are REAL; the `!=` disk-store branch is `set_relation_num_checks` (uninstalled ⇒ panic) because the trimmed PgClassForm carries no relchecks and the typed model has no pg_class relchecks-set carrier. This is the task-specified seam boundary, not absent logic. |
| `RemoveAttributeById` (1683) | constraints.rs `RemoveAttributeById` | SEAMED (mirror-and-panic) + real RemoveStatistics | The relation-open/syscache-copy/GETSTRUCT-mutate/heap_modify_tuple/CatalogTupleUpdate is `remove_attribute_by_id_update` (writable pg_attribute carrier keystone, uninstalled ⇒ panic). The `RemoveStatistics(relid, attnum)` half runs in-crate after the seam. Inward `RemoveAttributeById` seam INSTALLED. |
| `RelationClearMissing` (1964) | constraints.rs `RelationClearMissing` | SEAMED (mirror-and-panic) | natts read in-crate; the per-attr clear is `relation_clear_missing_update` (writable carrier keystone). Inward `relation_clear_missing` seam INSTALLED. |
| `StoreAttrMissingVal` (2030) | constraints.rs `StoreAttrMissingVal` | SEAMED (mirror-and-panic) | construct_array + writable pg_attribute update delegated to `store_attr_missing_val` (carrier keystone). |
| `SetAttrMissing` (2086) | not ported | n/a (out of task scope) | Task explicitly defers; needs OidFunctionCall3 F_ARRAY_IN + writable carrier. No call site in-tree. |
| `CopyStatistics` (3442) | statistics.rs `CopyStatistics` | MATCH | table_open(RowExclusiveLock) + ScanKey starelid=fromrelid + systable_beginscan(StatisticRelidAttnumInhIndexId, indexOK=true); per row: rewrite column 1 (starelid=torelid) via `heap_modify_tuple` (behaviorally identical to C's heap_copytuple+GETSTRUCT-overwrite — both yield a tuple differing only in starelid, and CatalogTupleInsertWithInfo inserts a fresh row so the reset t_self is irrelevant); lazy CatalogOpenIndexes on first row; CatalogTupleInsertWithInfo; CatalogCloseIndexes + table_close. |

## Constants verified vs C headers

- `CONSTRAINT_CHECK = 'c'`, `CONSTRAINT_NOTNULL = 'n'` (pg_constraint.h). ✓
- `FirstUnpinnedObjectId = 12000` (access/transam.h). ✓
- EXPR_KIND_GENERATED_COLUMN / EXPR_KIND_COLUMN_DEFAULT / EXPR_KIND_CHECK_CONSTRAINT (parsenodes.h enum). ✓
- COERCION_ASSIGNMENT / COERCE_IMPLICIT_CAST. ✓
- `Anum_pg_statistic_starelid = 1` (pg_statistic.h). ✓
- All SQLSTATEs match the C ereport calls (table above).

## Seam audit

Owned seam crate: `backend-catalog-heap-seams`. New declarations:
`set_relation_num_checks`, `merge_with_existing_constraint`,
`remove_attribute_by_id_update`, `relation_clear_missing_update`,
`store_attr_missing_val` — all are OUTWARD dependency seams (real owner = the
unported writable-pg_attribute / pg_constraint-update carrier layer), each
CALLED by this crate and intentionally uninstalled (mirror-and-panic). The
`every_declared_seam_is_installed_by_its_owner` guard exempts owner-called
outward seams; test passes.

Inward seams now INSTALLED by this crate's `init_seams()`:
`RemoveAttributeById`, `relation_clear_missing` (dependency.c / ALTER paths),
plus the cross-crate `add_relation_new_constraints` /
`add_relation_not_null_constraints` (declared in tablecmds-seams, owned here,
consumer in tablecmds is live). Seam bodies are thin marshal+delegate
(scratch MemoryContext for the mcx-less inward seams).

No computation/branching lives in a seam path. New deps verified cycle-free
(`cargo tree -i backend-catalog-heap` empty for each).

## Verdict

PASS for the task-scoped surface. Every ported function is MATCH; the SEAMED
verdicts are all task-specified carrier keystones (writable pg_attribute row,
pg_constraint field-update, pg_class relchecks-set) that delegate to loud
panics, not absent logic. The one field-level limitation (StoreConstraints
inhcount round-trip) is on an unexercised NIL-only branch and is documented.
Workspace check + seams-init guards + no-todo guard all green.
