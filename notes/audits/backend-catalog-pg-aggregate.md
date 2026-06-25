# Audit: backend-catalog-pg-aggregate (catalog/pg_aggregate.c)

Status: **audited** — both C functions ported in full, branch-for-branch. No `todo!`/`unimplemented!`. All deferrals are real seam `::call` into their owner crates (mirror-PG-and-panic) or genuine `ereport(ERROR)` mapped through `PgResult` with the post-ereport `unreachable!()`/`?`-propagation idiom.

C source: `src/backend/catalog/pg_aggregate.c` (916 LOC, 2 functions). Ported function-by-function against PostgreSQL 18.3.

## Per-function parity

### AggregateCreate (C 45-812) — MATCH

Verified line-for-line against the C:

- **Sanity checks** (C 111-132): `!aggName` → `elog`; `!aggtransfnName` → `elog`; `numDirectArgs<0 || >numArgs` → `elog`; `numArgs<0 || >FUNC_MAX_ARGS-1` → `ereport(TOO_MANY_ARGUMENTS, errmsg_plural)`. The `aggName`-empty test mirrors C's `!aggName` (the owned bundle carries `agg_name: String`; empty == the C NULL/empty contract).
- **Polymorphic transtype / mtranstype** (C 134-160): `check_valid_polymorphic_signature` twice, each guarded by `OidIsValid(aggmTransType)` for the second; `INVALID_FUNCTION_DEFINITION` + `errdetail_internal`.
- **Ordered-set VARIADIC must be ANY** (C 162-173): `FEATURE_NOT_SUPPORTED`.
- **Hypothetical-set direct/aggregated arg match** (C 175-200): the `memcmp` becomes a slice `!=` over the same offsets/count; same triple-OR condition.
- **transfn lookup** (C 202-231): ordered-set vs ordinary `nargs_transfn` + `fnArgs` fill via `copy_from_slice` mirroring the two `memcpy`s; `lookup_agg_function`.
- **transfn return-type == transtype** (C 233-248) and the **PROCOID strictness/initval** check (C 250-269): `pg_proc_form` (SearchSysCache1 PROCOID) → `proisstrict`; missing → `elog("cache lookup failed for function %u")`. `ReleaseSysCache` = owned form drop.
- **mtransfn** (C 271-316), **minvtransfn** (C 318-355): re-use `fnArgs` with `fnArgs[0]` swapped; return-type checks; minvtransfn strictness-agreement check; `mtransIsStrict` tracked.
- **finalfn** (C 357-405): `finalfnExtraArgs` arg-count branch + `ffnVariadicArgType` reset; `func_strict` extra-args STRICT check; else `finaltype = aggTransType`.
- **combinefn** (C 407-441): 2-arg both transtype; return-type == transtype; INTERNAL + strict check with `format_type_be`.
- **serialfn** (C 443-461): `serialize(internal) returns bytea`. **deserialfn** (C 463-482): `deserialize(bytea, internal) returns internal`.
- **result polymorphic / internal signature** (C 484-514): `check_valid_polymorphic_signature(finaltype)` (DATATYPE_MISMATCH) + `check_valid_internal_signature(finaltype)` (INVALID_FUNCTION_DEFINITION).
- **moving-aggregate finalfn + result-type match** (C 516-570): mfinalfn lookup figured like the regular finalfn but with `aggmTransType`/`mfinalfnExtraArgs`; `rettype != finaltype` → INVALID_FUNCTION_DEFINITION with both `format_type_be`s.
- **sortop** (C 572-582): single-arg-only check; `LookupOperName` via `lookup_oper_name`.
- **type ACL checks** (C 584-607): `object_aclcheck(TypeRelationId, …, GetUserId(), ACL_USAGE)` for each arg type, transtype, mtranstype (if valid), finaltype; `aclcheck_error_type` on failure.
- **ProcedureCreate** (C 610-645): all fixed aggregate arguments reproduced exactly (returnsSet=false, validator=Invalid, prosrc="aggregate_dummy", probin/prosqlbody=NULL, PROKIND_AGGREGATE, security/leakproof/strict=false, PROVOLATILE_IMMUTABLE, trftypes=NULL, trfoids=NIL, proconfig=NULL, prosupport=Invalid, procost=1, prorows=0); `language_oid = INTERNALlanguageId (12)`; `proowner = GetUserId()`. `procOid = myself.objectId`.
- **pg_aggregate row** (C 647-687): `FormData_pg_aggregate` fixed columns in catalog order + the two nullable `agginitval`/`aggminitval` text columns (`CStringGetTextDatum` when present, else NULL). Every value placement matches the `values[Anum_… - 1]` assignments 1:1.
- **replace-vs-insert** (C 689-732): `if replace` → `SearchSysCache1(AGGFNOID, procOid)` via `aggregate_tuple_by_fnoid` (returns held tuple + `AggRow`); aggkind-change → `WRONG_OBJECT_TYPE` with the three-way `errdetail`; aggnumdirectargs-change → `INVALID_FUNCTION_DEFINITION`; `replaces[aggfnoid/aggkind/aggnumdirectargs]=false` → `catalog_tuple_update_pg_aggregate`. Else `catalog_tuple_insert_pg_aggregate`.
- **dependency recording** (C 734-811): `table_close`; then `new_object_addresses` + `add_exact_object_address` for transfn (always) + finalfn/combinefn/serialfn/deserialfn/mtransfn/minvtransfn/mfinalfn (each `OidIsValid`-guarded, ProcedureRelationId) + sortop (OperatorRelationId); `record_object_address_dependencies(&myself, addrs, DEPENDENCY_NORMAL)`; `free_object_addresses` = `Vec` drop. Returns `myself`.

### lookup_agg_function (C 826-915, file-static) — MATCH

- `func_get_detail(fnName, NIL, NIL, nargs, input_types, false,false,false, …)` (C 849-853) via the `func_get_detail` seam, with empty fargs/fargnames and the three `false` flags encoded by the seam contract.
- `fdresult != FUNCDETAIL_NORMAL || !OidIsValid(fnOid)` → `UNDEFINED_FUNCTION` "function %s does not exist" (C 855-861).
- `retset` → DATATYPE_MISMATCH "returns a set" (C 862-867).
- `variadicArgType==ANYOID && vatype!=ANYOID` → DATATYPE_MISMATCH (C 869-882).
- `enforce_generic_type_consistency(input_types, true_oid_array, nargs, *rettype, true)` mutating `true_oid_array` and refining `rettype` (C 884-893).
- binary-coercible loop over `nargs` (C 895-907): `IsBinaryCoercible(input_types[i], true_oid_array[i])`; failure → DATATYPE_MISMATCH "requires run-time type coercion" with `func_signature_string(…, true_oid_array)`.
- `object_aclcheck(ProcedureRelationId, fnOid, GetUserId(), ACL_EXECUTE)` → `aclcheck_error(…, OBJECT_FUNCTION, get_func_name(fnOid))` via `aclcheck_error_function` (C 909-912).
- Returns `(fnOid, rettype)` (the C return + `*rettype` out-param).

All six shared return-type-mismatch errors funnel through one helper `datatype_mismatch_return_type` reproducing the C `errmsg("return type of %s function %s is not %s")` with the per-call "transition"/"inverse transition"/"combine"/"serialization"/"deserialization" kind and the wanted type via `format_type_be`. The C spells each inline; the message text/SQLSTATE are identical.

## pg_aggregate.h constant verification (types-catalog::pg_aggregate)

Field-verified against `src/include/catalog/pg_aggregate.h`:

- `AggregateRelationId = 2600`, `AggregateFnoidIndexId = 2650`.
- 22 `Anum_pg_aggregate_*` in field order: aggfnoid(1), aggkind(2), aggnumdirectargs(3), aggtransfn(4), aggfinalfn(5), aggcombinefn(6), aggserialfn(7), aggdeserialfn(8), aggmtransfn(9), aggminvtransfn(10), aggmfinalfn(11), aggfinalextra(12), aggmfinalextra(13), aggfinalmodify(14), aggmfinalmodify(15), aggsortop(16), aggtranstype(17), aggtransspace(18), aggmtranstype(19), aggmtransspace(20), agginitval(21), aggminitval(22).
- `Natts_pg_aggregate = 22` (20 fixed + 2 `CATALOG_VARLEN` text columns).
- `FormData_pg_aggregate` carries the 20 fixed columns with the C column types: `regproc`/`Oid` → `Oid`; `char` → `i8`; `int16` → `i16`; `int32` → `i32`; `bool` → `bool`.
- `AGGKIND_NORMAL='n'`, `AGGKIND_ORDERED_SET='o'`, `AGGKIND_HYPOTHETICAL='h'`, `AGGKIND_IS_ORDERED_SET(kind) = kind != 'n'`.

## Catalog-write keystone (indexing.c F1)

The pg_aggregate row form+insert / modify+update is owned by `catalog/indexing.c`. Two new typed seams declared in `backend-catalog-indexing-seams` and **installed** by the indexing owner (`family1.rs::install` / `aggregate_values`):

- `catalog_tuple_insert_pg_aggregate` — `heap_form_tuple(rd_att, values, nulls)` + `CatalogTupleInsert` (the C 730-731 fresh path). No OID column (key `aggfnoid` is the pre-assigned pg_proc OID).
- `catalog_tuple_update_pg_aggregate` — `heap_modify_tuple(oldtup, rd_att, values, nulls, replaces)` + `CatalogTupleUpdate(&tup->t_self)` (the C 724-725 REPLACE path).

The `agginitval`/`aggminitval` text columns use `CStringGetTextDatum` (4-byte varlena header + payload) carried as `Datum::ByRef`; NULL ⇒ `nulls[Anum-1]=true`.

## Cross-owner seams

ProcedureCreate (pg_proc.c) crosses through **`backend-commands-functioncmds-seams::procedure_create`** — the seam's established home (also consumed by functioncmds.c for the identical C call). The pg_proc owner is being ported **concurrently in another lane and is not yet merged**, so the seam is currently uninstalled → mirror-PG-and-panic. Reusing the existing seam (rather than declaring a competing `backend-catalog-pg-proc-seams::ProcedureCreate`) avoids a duplicate declaration the pg_proc lane would have to install twice; when that lane lands and installs `procedure_create`, both AggregateCreate and functioncmds work. (The `seams-init` "every declared seam installed by its owner" guard exempts it: its owner crate is not present/complete.)

New seam declarations added (first-consumer home = the C function's real owner unit):

- `func_get_detail` + `FuncDetail`/`FuncDetailCode` → `backend-parser-parse-func-seams` (parse_func.c owner).
- `check_valid_polymorphic_signature`, `check_valid_internal_signature` → `backend-parser-coerce-seams` (parse_coerce.c owner).
- `aggregate_tuple_by_fnoid` → `backend-utils-cache-syscache-seams` (SearchSysCache1 AGGFNOID held tuple + `AggRow`).

Reused existing seams: `is_binary_coercible` / `enforce_generic_type_consistency` (coerce-seams), `func_signature_string` / `name_list_to_string` / `get_user_id` / `aclcheck_error_function` (functioncmds-seams), `lookup_oper_name` (parse-oper-seams), `format_type_be` (format-type-seams), `func_strict` / `get_func_name` (lsyscache-seams), `pg_proc_form` (syscache-seams, PROCOID `proisstrict`), `object_aclcheck` / `aclcheck_error_type` (aclchk-seams).

Direct deps (no cycle): `backend-catalog-dependency` (new/add_exact/record_object_address_dependencies), `backend-access-table-table` (table_open / Relation::close).

## Inward seam (this owner installs)

`backend-catalog-pg-aggregate-seams::aggregate_create` — the `AggregateCreateArgs` bundle `DefineAggregate` (aggregatecmds.c) already builds; previously a mirror-PG-and-panic, now installed from `init_seams()`. `init_seams()` wired into `seams-init::init_all`.

## Notes / faithful deviations

- `lookup_agg_function`'s `input_types` is the caller's `fnArgs` buffer; only the first `nargs` entries are meaningful (C indexes `input_types[i]`/passes `input_types` with `nargs`), so the port slices `&fnArgs[..nargs]` before crossing the seams.
- The `SearchSysCache1(PROCOID)`/`ReleaseSysCache` pair is a scoped owned form (`pg_proc_form`) whose drop is the release; only `proisstrict` is read, matching the C `GETSTRUCT(Form_pg_proc)->proisstrict` use.
- The replace path reads `aggkind`/`aggnumdirectargs` off the projected `AggRow` (the held `oldtup`'s `GETSTRUCT`) before the `heap_modify_tuple`, exactly the C order.

## Gate

- `cargo check --workspace` GREEN.
- `cargo test -p no-todo-guard` GREEN (no `todo!`/`unimplemented!`).
- `cargo test -p seams-init` GREEN — both recurrence guards (`every_seam_installing_crate_is_wired_into_init_all`, `every_declared_seam_is_installed_by_its_owner`).
- `cargo clippy -p backend-catalog-pg-aggregate` / `-p backend-catalog-indexing` clean (C-shape allows documented in the crate attrs).
