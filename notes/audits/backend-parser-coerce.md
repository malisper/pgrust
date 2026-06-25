# Audit — backend-parser-coerce

Unit `backend-parser-coerce` ports `parser/parse_coerce.c` (PostgreSQL 18.3,
~3402 LOC). STEP 3 of the parser parse-analysis campaign (route to #159).

Re-derived independently from the C and the c2rust rendering. The crate operates
over the split raw-`Node`/lifetime-free-`Expr` model: coercion takes an
already-transformed `Expr` and adds decoration on top.

## Leftover-directory reconciliation

`origin/main` had NO `crates/backend-parser-coerce` directory (CATALOG status
`todo`). The only artifact was `crates/backend-parser-coerce-seams` (the outward
seam decls that parse_expr.c / parse_oper.c already consume). No broken stub
existed in the tree — this is a fresh, full port, not a repair.

## New infrastructure added for this unit (verified vs C headers)

- `types-tuple::heaptuple::RECORDARRAYOID = 2287` (pg_type_d.h).
- `syscache-seams::CastRow` + `cast_by_source_target(src,tgt)` — full
  `Form_pg_cast` projection `(oid, castfunc, castcontext, castmethod)` via
  `SearchSysCache2(CASTSOURCETARGET, …)`. INSTALLED by the syscache owner
  (`projections.rs`), attnums verified vs pg_cast.h (oid=1, castfunc=4,
  castcontext=5, castmethod=6). `find_coercion_pathway` /
  `IsBinaryCoercibleWithCast` / `find_typmod_coercion_function` use it.
- `syscache-seams::search_relation_reloftype(relid)` — `Form_pg_class.reloftype`
  (attnum 5 verified vs pg_class.h), INSTALLED by the syscache owner. Used by
  `typeIsOfTypedTable`.
- `pg-inherits-seams::type_inherits_from(sub,super)` — `typeInheritsFrom`
  (pg_inherits.c:406). Owner `backend-catalog-pg-inherits` is `todo`, so the
  seam panics until it lands (sanctioned mirror-PG-and-panic; the guard exempts
  unported owners).

## parse_coerce.c

| C function | C loc | Port | Verdict | Notes |
|---|---|---|---|---|
| `coerce_to_target_type` | 77 | `coerce_to_target_type` | MATCH | `can_coerce_type` gate → NULL/`None`; strip top CollateExpr(s), `coerce_type`, `coerce_type_typmod` with `hideInputCoercion = (result != expr && !IsA Const)`, reinstall top CollateExpr iff target collatable. C pointer-identity `result != expr` modeled by `exprs_identical` (structural Debug compare — coerce_type returns the *unchanged* value only in its no-op arms, which are byte-identical, so the implicit-display-form decision is faithful). `location` set on the CollateExpr is C `coll->location`; trimmed `CollateExpr` carries no location (behavior-preserving). |
| `coerce_type` | 157 | `coerce_type` | MATCH (one arm panics) | All arms ported: same-type/NULL; ANY/ANYELEMENT/ANYNONARRAY/ANYCOMPATIBLE[NONARRAY] return-as-is; ANYARRAY/ANYENUM/ANYRANGE/ANYMULTIRANGE/ANYCOMPATIBLE{ARRAY,RANGE,MULTIRANGE} domain-relabel; CollateExpr push-under; `find_coercion_pathway` FUNC/RELABELTYPE branches with domain `coerce_to_domain`; RECORD↔composite; record[]; `typeInheritsFrom`/`typeIsOfTypedTable` ConvertRowtypeExpr; final `elog(ERROR)`. UNKNOWN-`Const` arm (line 232) mirror-PG-and-panics: building a typed Const from an UNKNOWN literal needs `DatumGetCString(con->constvalue)` + `stringTypeDatum`, both blocked on the execTuples canonical-carrier (#113) — the trimmed `Const` stores a bare-word `Datum<'static>` with no cstring decode/store path. Param `p_coerce_param_hook` arm (line 372) panics: the hook returns a raw `NodePtr`, which the Expr coercion path cannot consume (raw-Node/Expr split keystone); no installed parser hook reaches it. |
| `can_coerce_type` | 556 | `can_coerce_type` | MATCH | Per-arg: same-type, ANY, polymorphic (defer), UNKNOWN, `find_coercion_pathway`, RECORD↔composite, record[], inherits/typed-table; `have_generics` → `check_generic_type_consistency`. |
| `coerce_to_domain` | 674 | `coerce_to_domain` | MATCH | `Assert(OidIsValid(baseTypeId))`; not-a-domain return-as-is; `hide_coercion_node` when `hideInputCoercion`; `coerce_type_typmod`(IMPLICIT); build `CoerceToDomain{resulttypmod:-1, resultcollid:Invalid}`. |
| `coerce_type_typmod` | 750 | `coerce_type_typmod` | MATCH | typmod-already-done skip via `exprTypmod`; hide; `targetTypMod<0` → NONE else `find_typmod_coercion_function`; FUNC → `build_coercion_expression`, else `applyRelabelType(node,…,exprCollation(node),…)`. |
| `hide_coercion_node` | 808 | `hide_coercion_node` | MATCH | FuncExpr/RelabelType/CoerceViaIO/ArrayCoerceExpr/ConvertRowtypeExpr/RowExpr/CoerceToDomain format→IMPLICIT; else `elog(ERROR) unsupported node type`. (`nodeTag` numeric value not modeled — owned enum has no tag table; the message is the only consumer.) |
| `build_coercion_expression` | 836 | `build_coercion_expression` | MATCH | `proc_row_by_oid` for `pronargs` (the Asserts on proretset/prokind are debug-only, omitted); FUNC → FuncExpr + int4 typmod const (nargs≥2) + bool isExplicit const (nargs==3); ARRAYCOERCE → CaseTestExpr source-elem + recursive `coerce_to_target_type` + ArrayCoerceExpr; COERCEVIAIO → CoerceViaIO; else `elog(ERROR)`. Const values built via `make_const` (by-value words). |
| `coerce_record_to_complex` | 1009 | `coerce_record_to_complex` | MATCH (Var arm panics) | RowExpr-input arm full: `lookup_rowtype_tupdesc`, per-attr dropped→null-const / `coerce_to_target_type` (pstate threaded) with too-few/too-many/can't-cast errors, RowExpr build, domain `coerce_to_domain`. Whole-row-Var arm (line 1034) mirror-PG-and-panics: `expandNSItemVars` yields the raw `NodePtr` universe, unwalkable by the Expr coercion path (raw-Node/Expr split keystone). Non-Row/non-Var input → `ERRCODE_CANNOT_COERCE`. |
| `coerce_to_boolean` | 1158 | `coerce_to_boolean` | MATCH | `exprType != BOOL` → `coerce_to_target_type`(ASSIGNMENT) or `ERRCODE_DATATYPE_MISMATCH`; `expression_returns_set` → error. |
| `coerce_to_specific_type_typmod` | 1205 | `coerce_to_specific_type_typmod` | MATCH | As boolean but to `targetTypeId`/`targetTypmod`. |
| `coerce_to_specific_type` | 1254 | `coerce_to_specific_type` | MATCH | Delegates with typmod -1. |
| `coerce_null_to_domain` | 1270 | `coerce_null_to_domain` | MATCH | `getBaseTypeAndTypmod` → `makeConst(isnull=true)` → domain `coerce_to_domain`(IMPLICIT). |
| `parser_coercion_errposition` | 1311 | `parser_coercion_errposition` | MATCH | `coerce_location>=0` → `parser_errposition(loc)` else `parser_errposition(exprLocation(input))`. |
| `select_common_type` | 1341 | `select_common_type` | MATCH | All-same fast path (only domain-preserving path); else base-type + category/preferred loop; cross-category → `context==None`?InvalidOid:error; preferred-coercibility swap; all-UNKNOWN→TEXT. `which_expr` out-param dropped (NULL at all sites). |
| `select_common_type_from_oids` | 1477 | `select_common_type_from_oids` | MATCH | Same logic over an OID array; `noerror` controls error vs InvalidOid. |
| `coerce_to_common_type` | 1571 | `coerce_to_common_type` | MATCH | same-type no-op; `can_coerce_type`(IMPLICIT) → `coerce_type` else `ERRCODE_CANNOT_COERCE`. |
| `verify_common_type` | 1605 | `verify_common_type` | MATCH | per-expr `can_coerce_type` to common_type. |
| `verify_common_type_from_oids` | 1625 | `verify_common_type_from_oids` | MATCH | OID-array variant. |
| `select_common_typmod` | 1643 | `select_common_typmod` | MATCH | first typmod, fall to -1 on any type/typmod mismatch. (pstate param unused in C; dropped.) |
| `check_generic_type_consistency` | 1736 | `check_generic_type_consistency` | MATCH | Full family-1 (ANYELEMENT/NONARRAY/ENUM/ARRAY/RANGE/MULTIRANGE) + family-2 (ANYCOMPATIBLE*) consistency, domain-flattening, array/range/multirange element deduction, ANYARRAY special-case, anynonarray/anyenum checks, anycompatible common-supertype via `select_common_type_from_oids(…,true)` + `verify_common_type_from_oids`. Non-erroring (`Ok(false)`). |
| `enforce_generic_type_consistency` | 2130 | `enforce_generic_type_consistency` | MATCH | Full mutation of `declared_arg_types` + result-type resolution; family-1 + family-2; `allow_poly` polymorphic-actual handling; UNKNOWN re-scan; every `ereport(ERROR)` (not-all-alike/not-an-array/not-a-range/not-a-multirange/not-consistent/could-not-determine/could-not-find-array/identify-* internal). |
| `check_valid_polymorphic_signature` | 2874 | `check_valid_polymorphic_signature` | MATCH | Returns `Option<String>` (None=valid); ANYRANGE/MULTIRANGE, ANYCOMPATIBLE{RANGE,MULTIRANGE}, family-1, family-2 require-an-input checks; `psprintf` text 1:1. |
| `check_valid_internal_signature` | 2951 | `check_valid_internal_signature` | MATCH | INTERNAL rettype requires an INTERNAL input. |
| `TypeCategory` | 2975 | `TypeCategory` | MATCH | `get_type_category_preferred` typcategory; `Assert != INVALID`. |
| `IsPreferredType` | 2994 | `IsPreferredType` | MATCH | category match or INVALID-wildcard → typispreferred. |
| `IsBinaryCoercible` | 3029 | `IsBinaryCoercible` | MATCH | delegates to `IsBinaryCoercibleWithCast`. |
| `IsBinaryCoercibleWithCast` | 3044 | `IsBinaryCoercibleWithCast` | MATCH | same-type/ANY/ANYELEMENT/ANYCOMPATIBLE; domain→base; array/nonarray/enum/range/multirange/RECORD/RECORD[] pseudo-target acceptance; pg_cast BINARY+IMPLICIT → castoid. Returns `(bool, Oid)`. |
| `find_coercion_pathway` | 3152 | `find_coercion_pathway` | MATCH | domain→base both sides; same-type→RELABELTYPE; pg_cast castcontext char→enum + `ccontext>=castcontext` (integer rank) + castmethod→FUNC/COERCEVIAIO/RELABELTYPE; no-cast array-pair recursion→ARRAYCOERCE (oidvector/int2vector hack); assignment/explicit string-category I/O; PLPGSQL fallback. |
| `find_typmod_coercion_function` | 3315 | `find_typmod_coercion_function` | MATCH | true-array (via `get_element_type`, which yields the element only for `IsTrueArrayType`) → element + ARRAYCOERCE; self→self pg_cast `castfunc`; no funcid → NONE. |
| `is_complex_array` | 3365 | `is_complex_array` | MATCH | `get_element_type` valid && `ISCOMPLEX(elem)`. |
| `typeIsOfTypedTable` | 3379 | `typeIsOfTypedTable` | MATCH | `typeOrDomainTypeRelid` (parse_type) → `pg_class.reloftype` via `search_relation_reloftype` == reloftypeId; missing relation → `elog(ERROR)`. |

### Macro / inline helpers

- `ISCOMPLEX(typeid)` = `typeOrDomainTypeRelid(typeid) != InvalidOid`
  (parse_type.h:59) — `is_complex` ported exactly (NOT `get_typtype`).
- `type_is_array(typid)` = `get_element_type != InvalidOid`;
  `type_is_array_domain(typid)` = `get_base_element_type != InvalidOid`
  (lsyscache.h macros) — ported inline.
- `IsPolymorphicType{,Family1,Family2}` — ported as `matches!` over the ANY*OID
  constant set (verified vs pg_type_d.h, shared with parse_oper's copy).
- `CoercionContext` integer ordering (`ccontext >= castcontext`) via `as i32`
  (enum repr 0/1/2/3 verified: IMPLICIT<ASSIGNMENT<PLPGSQL<EXPLICIT).

## Seams installed (inward; consumed by parse_expr.c / parse_oper.c)

`init_seams` installs all 9 coerce-seams decls:
`find_coercion_pathway_implicit`, `is_binary_coercible`,
`enforce_generic_type_consistency`, `coerce_to_boolean`,
`coerce_to_specific_type`, `coerce_to_common_type`, `select_common_type`,
`verify_common_type`, `coerce_to_target_type`. Wired into
`seams-init::init_all`. The seams carry only `&mut ParseState` (C signatures);
ParseState holds no `Mcx`, so each wrapper opens a scratch `MemoryContext` for
the transient allocations (the produced `Expr` tree is lifetime-free, so it
outlives the scratch).

## residual_own_todos = 0

No `todo!()`/`unimplemented!()`. Two genuinely-blocked arms are sanctioned
mirror-PG-and-panic (named keystones): the `coerce_type` UNKNOWN-`Const` literal
conversion (execTuples canonical-carrier #113) and the
`coerce_record_to_complex` whole-row-`Var` expansion (raw-Node/Expr split). The
`p_coerce_param_hook` arm likewise panics (raw-NodePtr hook bridge); no installed
hook reaches it. All other logic is fully ported.

## Gate

`cargo check --workspace` clean; `cargo test -p no-todo-guard` /
`-p seams-init` / `-p backend-parser-coerce` (4 tests) pass; `cargo test
--workspace` green except the pre-existing allowed flake
`backend-optimizer-path-small::range_pair_positive_combination`.

Verdict: PASS.

## Parity-fix sweep (RelabelType/FuncExpr location)

Following the F0 #219 primnode `location` fields landing, `coerce_type` /
`build_coercion_expression` now set `->location = location` on the nodes they
build, matching parse_coerce.c:225 / :474 / :533 / :915:

- ANYARRAY-target RelabelType (parse_coerce.c:220-225)
- domain-base RelabelType (parse_coerce.c:469-474)
- typed-table inheritance RelabelType (parse_coerce.c:528-533)
- `Coerceviaio`/`Func` path FuncExpr (parse_coerce.c:913-915)

`makeRelabelType`/`makeFuncExpr` (nodes-core) still set `location = -1`,
exactly as the C makers; the coerce callers stamp the real location. The
`coerce_type_typmod` `applyRelabelType(..., location, ...)` leg (nodeFuncs.c)
remains `location = -1` — `apply_relabel_type` has no `rlocation` parameter
yet, a pre-existing documented cross-crate deferral (ripples the
`ec_seam::apply_relabel_type` contract + equivclass/clauses consumers), out of
scope for this parser sweep.
