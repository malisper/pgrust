#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]
// The C nests `if (proc->proisstrict && initval == NULL) { if (...) ereport }`;
// keep that shape.
#![allow(clippy::collapsible_if)]
// The C bounds checks are spelled as two comparisons
// (`x < 0 || x > FUNC_MAX_ARGS - 1`); keep that 1:1 instead of a range-contains.
#![allow(clippy::manual_range_contains)]
// The per-type ACL loop indexes `aggArgTypes` by `i` exactly as the C `for`
// loop does; keep the C shape.
#![allow(clippy::needless_range_loop)]
// The C declares all locals (`Oid transfn;` etc.) up front, then assigns them
// later in the body; keep that 1:1 declaration order.
#![allow(clippy::needless_late_init)]
// Every fallible function returns the shared `PgResult`, whose `PgError`
// variant is large; boxing it would diverge from the workspace vocabulary and
// from the C (which returns/throws by value).
#![allow(clippy::result_large_err)]

//! Idiomatic port of `backend/catalog/pg_aggregate.c` — routines to support
//! manipulation of the `pg_aggregate` relation.
//!
//! Faithful 1:1 port of both C functions:
//!
//!   * [`AggregateCreate`] (pg_aggregate.c:45-812) — the public entry point: the
//!     sanity checks and polymorphism/transtype validations, the resolution of
//!     the transition / final / combine / serial / deserial / moving functions
//!     and their argument types against the aggregate's signature (each through
//!     [`lookup_agg_function`]), the support-function strictness rules, the
//!     type/permission ACL checks, the underlying `pg_proc` aggregate-impl entry
//!     creation via `ProcedureCreate`, the `pg_aggregate` tuple build and
//!     insert/update, and the extra `pg_proc`/operator dependency recording.
//!   * `lookup_agg_function` (pg_aggregate.c:826-915, file-static) — the common
//!     support-function resolver: the `func_get_detail` lookup, the
//!     normal/non-set/`VARIADIC ANY`/binary-coercible/result-type-consistency
//!     checks, and the `ACL_EXECUTE` permission check.
//!
//! Original branch order, validation, error codes/messages/SQLSTATE,
//! `values[]`/`nulls[]`/`replaces[]` field-formation order, the
//! replace-vs-insert decision, and the dependency-recording order are preserved.
//!
//! ## Shape of this port
//!
//!   * The decision logic runs in-crate over owned values.
//!   * `dependency.c`'s `ObjectAddresses` collection API
//!     (`new_object_addresses` / `add_exact_object_address` /
//!     `record_object_address_dependencies`) is called directly; the C
//!     `free_object_addresses` is the owned `Vec` dropping at end of scope.
//!     `access/table`'s `table_open` / `Relation::close` are called directly.
//!   * The catalog-tuple value layer (`heap_form_tuple` / `heap_modify_tuple` /
//!     `CatalogTupleInsert`/`Update`) is owned by `catalog/indexing.c` and
//!     crosses through that owner's `-seams` crate.
//!   * `ProcedureCreate` (`pg_proc.c`) crosses through
//!     `backend-commands-functioncmds-seams::procedure_create` (the seam's
//!     established home, also consumed by functioncmds.c for the identical C
//!     call). It loud-panics until the pg_proc owner installs it.
//!   * The polymorphism / coercion helpers (`check_valid_polymorphic_signature`
//!     / `check_valid_internal_signature` / `IsBinaryCoercible` /
//!     `enforce_generic_type_consistency` — parse_coerce.c), `func_get_detail` /
//!     `func_signature_string` (parse_func.c), `LookupOperName` (parse_oper.c),
//!     `func_strict` / `get_func_name` (lsyscache.c), `format_type_be`
//!     (format_type.c), `NameListToString` (namespace.c), the `GetUserId` /
//!     ACL checks (miscinit.c/aclchk.c), and the `SearchSysCache1`
//!     (PROCOID/AGGFNOID) probes all cross their owners' seams.

extern crate alloc;
use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;

use ::mcx::MemoryContext;

use ::utils_error::{elog, ereport};
use ::types_error::{
    PgError, PgResult, ERRCODE_DATATYPE_MISMATCH, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INVALID_FUNCTION_DEFINITION, ERRCODE_TOO_MANY_ARGUMENTS, ERRCODE_UNDEFINED_FUNCTION,
    ERRCODE_WRONG_OBJECT_TYPE, ERROR,
};

use ::types_core::primitive::{InvalidOid, Oid, OidIsValid, FUNC_MAX_ARGS};
use ::types_acl::acl::{AclResult, ACLCHECK_OK, ACL_EXECUTE, ACL_USAGE};
use ::types_catalog::catalog::{OPERATOR_RELATION_ID, PROCEDURE_RELATION_ID, TYPE_RELATION_ID};
use ::types_catalog::catalog_dependency::{ObjectAddress, DependencyType, DEPENDENCY_NORMAL};
use ::types_catalog::pg_aggregate::{
    AggregateRelationId, FormData_pg_aggregate, PgAggregateInsertRow, PgAggregateReplaces,
    AGGKIND_HYPOTHETICAL, AGGKIND_IS_ORDERED_SET, AGGKIND_NORMAL, AGGKIND_ORDERED_SET,
};
use ::types_storage::lock::RowExclusiveLock;
use ::types_tuple::heaptuple::{ANYOID, BYTEAOID, INTERNALOID};

use ::table::table_open;
use ::dependency::{
    add_exact_object_address, new_object_addresses, record_object_address_dependencies,
};

use ::aclchk_seams::{aclcheck_error_type, object_aclcheck};
use indexing_seams as indexing_seams;
use ::functioncmds_seams::{
    aclcheck_error_function, func_signature_string, get_user_id, name_list_to_string,
    procedure_create, ProcedureCreateArgs,
};
use ::coerce_seams::{
    check_valid_internal_signature, check_valid_polymorphic_signature,
    enforce_generic_type_consistency, is_binary_coercible,
};
use ::parse_func_seams::{func_get_detail, FuncDetailCode};
use ::parse_oper_seams::lookup_oper_name;
use ::format_type_seams::format_type_be;
use ::lsyscache_seams::{func_strict, get_func_name};
use ::syscache_seams::{aggregate_tuple_by_fnoid, pg_proc_form};

use ::pg_aggregate_seams::AggregateCreateArgs;

/// `ProcedureRelationId` (`catalog/pg_proc.h`).
const ProcedureRelationId: Oid = PROCEDURE_RELATION_ID;
/// `OperatorRelationId` (`catalog/pg_operator.h`).
const OperatorRelationId: Oid = OPERATOR_RELATION_ID;
/// `TypeRelationId` (`catalog/pg_type.h`).
const TypeRelationId: Oid = TYPE_RELATION_ID;

/// `INTERNALlanguageId` (`catalog/pg_language.h`) — OID 12.
const INTERNALlanguageId: Oid = 12;
/// `PROKIND_AGGREGATE` (`catalog/pg_proc.h`).
const PROKIND_AGGREGATE: i8 = b'a' as i8;
/// `PROVOLATILE_IMMUTABLE` (`catalog/pg_proc.h`).
const PROVOLATILE_IMMUTABLE: i8 = b'i' as i8;

/// `ObjectAddressSet(object, classId, objectId)` (objectaddress.h): set
/// `classId`/`objectId` and zero `objectSubId`.
#[inline]
fn ObjectAddressSet(class_id: Oid, object_id: Oid) -> ObjectAddress {
    ObjectAddress {
        classId: class_id,
        objectId: object_id,
        objectSubId: 0,
    }
}

/// Flatten a possibly-qualified function-name list (the parser's `List *` of
/// `String` value nodes, here `&[Node]`) to the bare name components, mirroring
/// the C `fnName` `List *` passed straight through to `func_get_detail` /
/// `func_signature_string` / `NameListToString`. A non-`String` node becomes the
/// empty string (the value nodes the grammar produces are always `String`s).
fn name_list(names: &[parsenodes::Node]) -> Vec<String> {
    names
        .iter()
        .map(|n| match n.as_string() {
            Some(s) => s.sval.clone().unwrap_or_default(),
            None => String::new(),
        })
        .collect()
}

/// `elog(ERROR, msg)` — an internal (errmsg_internal) error with no SQLSTATE,
/// matching the C `elog(ERROR, ...)` calls in the sanity-check block.
fn elog_error(message: impl Into<String>) -> PgError {
    match elog(ERROR, message.into()) {
        Ok(()) => unreachable!("elog(ERROR) must not return Ok"),
        Err(e) => e,
    }
}

/* ===========================================================================
 * AggregateCreate (pg_aggregate.c:45-812)
 * ========================================================================= */

/// `AggregateCreate(...)` (pg_aggregate.c:45). The argument bundle is the
/// [`AggregateCreateArgs`] that `DefineAggregate` (aggregatecmds.c) builds.
pub fn AggregateCreate(args: AggregateCreateArgs) -> PgResult<ObjectAddress> {
    let AggregateCreateArgs {
        agg_name: aggName,
        agg_namespace: aggNamespace,
        replace,
        agg_kind: aggKind,
        num_args: numArgs,
        num_direct_args: numDirectArgs,
        parameter_types: parameterTypes,
        all_parameter_types: allParameterTypes,
        parameter_modes: parameterModes,
        parameter_names: parameterNames,
        parameter_defaults: parameterDefaults,
        variadic_arg_type: variadicArgType,
        transfunc_name: aggtransfnName,
        finalfunc_name: aggfinalfnName,
        combinefunc_name: aggcombinefnName,
        serialfunc_name: aggserialfnName,
        deserialfunc_name: aggdeserialfnName,
        mtransfunc_name: aggmtransfnName,
        minvtransfunc_name: aggminvtransfnName,
        mfinalfunc_name: aggmfinalfnName,
        finalfunc_extra_args: finalfnExtraArgs,
        mfinalfunc_extra_args: mfinalfnExtraArgs,
        finalfunc_modify: finalfnModify,
        mfinalfunc_modify: mfinalfnModify,
        sortoperator_name: aggsortopName,
        trans_type_id: aggTransType,
        trans_space: aggTransSpace,
        mtrans_type_id: aggmTransType,
        mtrans_space: aggmTransSpace,
        initval: agginitval,
        minitval: aggminitval,
        proparallel,
    } = args;

    let transfn: Oid;
    let mut finalfn: Oid = InvalidOid; /* can be omitted */
    let mut combinefn: Oid = InvalidOid; /* can be omitted */
    let mut serialfn: Oid = InvalidOid; /* can be omitted */
    let mut deserialfn: Oid = InvalidOid; /* can be omitted */
    let mut mtransfn: Oid = InvalidOid; /* can be omitted */
    let mut minvtransfn: Oid = InvalidOid; /* can be omitted */
    let mut mfinalfn: Oid = InvalidOid; /* can be omitted */
    let mut sortop: Oid = InvalidOid; /* can be omitted */
    /* Oid *aggArgTypes = parameterTypes->values; */
    let aggArgTypes: &[Oid] = &parameterTypes;
    let mut mtransIsStrict = false;
    let mut rettype: Oid;
    let finaltype: Oid;
    let mut fnArgs: [Oid; FUNC_MAX_ARGS] = [0; FUNC_MAX_ARGS];
    let nargs_transfn: i32;
    let mut nargs_finalfn: i32;
    let mut detailmsg: Option<String>;
    let mut referenced: ObjectAddress;
    let mut aclresult: AclResult;

    /* sanity checks (caller should have caught these) */
    if aggName.is_empty() {
        return Err(elog_error("no aggregate name supplied"));
    }
    if aggtransfnName.is_empty() {
        return Err(elog_error("aggregate must have a transition function"));
    }
    if numDirectArgs < 0 || numDirectArgs > numArgs {
        return Err(elog_error(
            "incorrect number of direct arguments for aggregate",
        ));
    }

    /*
     * Aggregates can have at most FUNC_MAX_ARGS-1 args, else the transfn and/or
     * finalfn will be unrepresentable in pg_proc.  We must check now to protect
     * fixed-size arrays here and possibly in called functions.
     */
    if numArgs < 0 || numArgs > FUNC_MAX_ARGS as i32 - 1 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_TOO_MANY_ARGUMENTS)
            .errmsg_plural(
                format!(
                    "aggregates cannot have more than {} argument",
                    FUNC_MAX_ARGS - 1
                ),
                format!(
                    "aggregates cannot have more than {} arguments",
                    FUNC_MAX_ARGS - 1
                ),
                (FUNC_MAX_ARGS - 1) as u64,
            )
            .into_error());
    }

    /*
     * If transtype is polymorphic, must have polymorphic argument also; else we
     * will have no way to deduce the actual transtype.
     */
    detailmsg = check_valid_polymorphic_signature::call(aggTransType, aggArgTypes, numArgs)?;
    if let Some(detailmsg) = detailmsg {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
            .errmsg("cannot determine transition data type")
            .errdetail_internal(detailmsg)
            .into_error());
    }

    /*
     * Likewise for moving-aggregate transtype, if any
     */
    if OidIsValid(aggmTransType) {
        detailmsg = check_valid_polymorphic_signature::call(aggmTransType, aggArgTypes, numArgs)?;
        if let Some(detailmsg) = detailmsg {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
                .errmsg("cannot determine transition data type")
                .errdetail_internal(detailmsg)
                .into_error());
        }
    }

    /*
     * An ordered-set aggregate that is VARIADIC must be VARIADIC ANY.  In
     * principle we could support regular variadic types, but it would make
     * things much more complicated because we'd have to assemble the correct
     * subsets of arguments into array values.  Since no standard aggregates have
     * use for such a case, we aren't bothering for now.
     */
    if AGGKIND_IS_ORDERED_SET(aggKind) && OidIsValid(variadicArgType) && variadicArgType != ANYOID {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("a variadic ordered-set aggregate must use VARIADIC type ANY")
            .into_error());
    }

    /*
     * If it's a hypothetical-set aggregate, there must be at least as many
     * direct arguments as aggregated ones, and the last N direct arguments must
     * match the aggregated ones in type.  (We have to check this again when the
     * aggregate is called, in case ANY is involved, but it makes sense to reject
     * the aggregate definition now if the declared arg types don't match up.)
     * It's unconditionally OK if numDirectArgs == numArgs, indicating that the
     * grammar merged identical VARIADIC entries from both lists.  Otherwise, if
     * the agg is VARIADIC, then we had VARIADIC only on the aggregated side,
     * which is not OK.  Otherwise, insist on the last N parameter types on each
     * side matching exactly.
     */
    if aggKind == AGGKIND_HYPOTHETICAL && numDirectArgs < numArgs {
        let numAggregatedArgs = numArgs - numDirectArgs;

        /*
         * memcmp(aggArgTypes + (numDirectArgs - numAggregatedArgs),
         *        aggArgTypes + numDirectArgs,
         *        numAggregatedArgs * sizeof(Oid)) != 0
         */
        let lhs_start = (numDirectArgs - numAggregatedArgs) as usize;
        let rhs_start = numDirectArgs as usize;
        let cnt = numAggregatedArgs as usize;
        if OidIsValid(variadicArgType)
            || numDirectArgs < numAggregatedArgs
            || aggArgTypes[lhs_start..lhs_start + cnt] != aggArgTypes[rhs_start..rhs_start + cnt]
        {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
                .errmsg("a hypothetical-set aggregate must have direct arguments matching its aggregated arguments")
                .into_error());
        }
    }

    /*
     * Find the transfn.  For ordinary aggs, it takes the transtype plus all
     * aggregate arguments.  For ordered-set aggs, it takes the transtype plus all
     * aggregated args, but not direct args.  However, we have to treat specially
     * the case where a trailing VARIADIC item is considered to cover both direct
     * and aggregated args.
     */
    if AGGKIND_IS_ORDERED_SET(aggKind) {
        if numDirectArgs < numArgs {
            nargs_transfn = numArgs - numDirectArgs + 1;
        } else {
            /* special case with VARIADIC last arg */
            debug_assert!(variadicArgType != InvalidOid);
            nargs_transfn = 2;
        }
        fnArgs[0] = aggTransType;
        /* memcpy(fnArgs + 1, aggArgTypes + (numArgs - (nargs_transfn - 1)), ...) */
        let src = (numArgs - (nargs_transfn - 1)) as usize;
        let cnt = (nargs_transfn - 1) as usize;
        fnArgs[1..1 + cnt].copy_from_slice(&aggArgTypes[src..src + cnt]);
    } else {
        nargs_transfn = numArgs + 1;
        fnArgs[0] = aggTransType;
        fnArgs[1..1 + numArgs as usize].copy_from_slice(&aggArgTypes[..numArgs as usize]);
    }
    let (tfn, tret) =
        lookup_agg_function(&aggtransfnName, nargs_transfn, &fnArgs, variadicArgType)?;
    transfn = tfn;
    rettype = tret;

    /*
     * Return type of transfn (possibly after refinement by
     * enforce_generic_type_consistency, if transtype isn't polymorphic) must
     * exactly match declared transtype.
     *
     * In the non-polymorphic-transtype case, it might be okay to allow a rettype
     * that's binary-coercible to transtype, but I'm not quite convinced that it's
     * either safe or useful.  When transtype is polymorphic we *must* demand exact
     * equality.
     */
    if rettype != aggTransType {
        return Err(datatype_mismatch_return_type(
            "transition",
            &aggtransfnName,
            aggTransType,
        )?);
    }

    /*
     * tup = SearchSysCache1(PROCOID, ObjectIdGetDatum(transfn));
     * if (proc->proisstrict && agginitval == NULL) { ... }
     */
    {
        let ctx = MemoryContext::new("AggregateCreate transfn proc");
        let proc = pg_proc_form::call(ctx.mcx(), transfn)?
            .ok_or_else(|| elog_error(format!("cache lookup failed for function {transfn}")))?;

        /*
         * If the transfn is strict and the initval is NULL, make sure first input
         * type and transtype are the same (or at least binary-compatible), so that
         * it's OK to use the first input value as the initial transValue.
         */
        if proc.proisstrict && agginitval.is_none() {
            if numArgs < 1 || !is_binary_coercible::call(aggArgTypes[0], aggTransType)? {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
                    .errmsg("must not omit initial value when transition function is strict and transition type is not compatible with input type")
                    .into_error());
            }
        }
        /* ReleaseSysCache(tup) — the owned form drops here. */
    }

    /* handle moving-aggregate transfn, if supplied */
    if !aggmtransfnName.is_empty() {
        /*
         * The arguments are the same as for the regular transfn, except that the
         * transition data type might be different.  So re-use the fnArgs values
         * set up above, except for that one.
         */
        debug_assert!(OidIsValid(aggmTransType));
        fnArgs[0] = aggmTransType;

        let (mfn, mret) =
            lookup_agg_function(&aggmtransfnName, nargs_transfn, &fnArgs, variadicArgType)?;
        mtransfn = mfn;
        rettype = mret;

        /* As above, return type must exactly match declared mtranstype. */
        if rettype != aggmTransType {
            return Err(datatype_mismatch_return_type(
                "transition",
                &aggmtransfnName,
                aggmTransType,
            )?);
        }

        let ctx = MemoryContext::new("AggregateCreate mtransfn proc");
        let proc = pg_proc_form::call(ctx.mcx(), mtransfn)?
            .ok_or_else(|| elog_error(format!("cache lookup failed for function {mtransfn}")))?;

        /*
         * If the mtransfn is strict and the minitval is NULL, check first input
         * type and mtranstype are binary-compatible.
         */
        if proc.proisstrict && aggminitval.is_none() {
            if numArgs < 1 || !is_binary_coercible::call(aggArgTypes[0], aggmTransType)? {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
                    .errmsg("must not omit initial value when transition function is strict and transition type is not compatible with input type")
                    .into_error());
            }
        }

        /* Remember if mtransfn is strict; we may need this below */
        mtransIsStrict = proc.proisstrict;
        /* ReleaseSysCache(tup) */
    }

    /* handle minvtransfn, if supplied */
    if !aggminvtransfnName.is_empty() {
        /*
         * This must have the same number of arguments with the same types as the
         * forward transition function, so just re-use the fnArgs data.
         */
        debug_assert!(!aggmtransfnName.is_empty());

        let (mifn, miret) =
            lookup_agg_function(&aggminvtransfnName, nargs_transfn, &fnArgs, variadicArgType)?;
        minvtransfn = mifn;
        rettype = miret;

        /* As above, return type must exactly match declared mtranstype. */
        if rettype != aggmTransType {
            return Err(datatype_mismatch_return_type(
                "inverse transition",
                &aggminvtransfnName,
                aggmTransType,
            )?);
        }

        let ctx = MemoryContext::new("AggregateCreate minvtransfn proc");
        let proc = pg_proc_form::call(ctx.mcx(), minvtransfn)?
            .ok_or_else(|| elog_error(format!("cache lookup failed for function {minvtransfn}")))?;

        /*
         * We require the strictness settings of the forward and inverse
         * transition functions to agree.  This saves having to handle assorted
         * special cases at execution time.
         */
        if proc.proisstrict != mtransIsStrict {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
                .errmsg(
                    "strictness of aggregate's forward and inverse transition functions must match",
                )
                .into_error());
        }
        /* ReleaseSysCache(tup) */
    }

    /* handle finalfn, if supplied */
    if !aggfinalfnName.is_empty() {
        /*
         * If finalfnExtraArgs is specified, the transfn takes the transtype plus
         * all args; otherwise, it just takes the transtype plus any direct args.
         * (Non-direct args are useless at runtime, and are actually passed as
         * NULLs, but we may need them in the function signature to allow
         * resolution of a polymorphic agg's result type.)
         */
        let mut ffnVariadicArgType = variadicArgType;

        fnArgs[0] = aggTransType;
        fnArgs[1..1 + numArgs as usize].copy_from_slice(&aggArgTypes[..numArgs as usize]);
        if finalfnExtraArgs {
            nargs_finalfn = numArgs + 1;
        } else {
            nargs_finalfn = numDirectArgs + 1;
            if numDirectArgs < numArgs {
                /* variadic argument doesn't affect finalfn */
                ffnVariadicArgType = InvalidOid;
            }
        }

        let (ffn, fret) =
            lookup_agg_function(&aggfinalfnName, nargs_finalfn, &fnArgs, ffnVariadicArgType)?;
        finalfn = ffn;
        finaltype = fret;

        /*
         * When finalfnExtraArgs is specified, the finalfn will certainly be passed
         * at least one null argument, so complain if it's strict.  Nothing bad
         * would happen at runtime (you'd just get a null result), but it's surely
         * not what the user wants, so let's complain now.
         */
        if finalfnExtraArgs && func_strict::call(finalfn)? {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
                .errmsg("final function with extra arguments must not be declared STRICT")
                .into_error());
        }
    } else {
        /*
         * If no finalfn, aggregate result type is type of the state value
         */
        finaltype = aggTransType;
    }
    debug_assert!(OidIsValid(finaltype));

    /* handle the combinefn, if supplied */
    if !aggcombinefnName.is_empty() {
        /*
         * Combine function must have 2 arguments, each of which is the trans type.
         * VARIADIC doesn't affect it.
         */
        fnArgs[0] = aggTransType;
        fnArgs[1] = aggTransType;

        let (cfn, combineType) = lookup_agg_function(&aggcombinefnName, 2, &fnArgs, InvalidOid)?;
        combinefn = cfn;

        /* Ensure the return type matches the aggregate's trans type */
        if combineType != aggTransType {
            return Err(datatype_mismatch_return_type(
                "combine",
                &aggcombinefnName,
                aggTransType,
            )?);
        }

        /*
         * A combine function to combine INTERNAL states must accept nulls and
         * ensure that the returned state is in the correct memory context. We
         * cannot directly check the latter, but we can check the former.
         */
        if aggTransType == INTERNALOID && func_strict::call(combinefn)? {
            let ctx = MemoryContext::new("AggregateCreate combinefn type");
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
                .errmsg(format!(
                    "combine function with transition type {} must not be declared STRICT",
                    format_type_be::call(ctx.mcx(), aggTransType)?.as_str()
                ))
                .into_error());
        }
    }

    /*
     * Validate the serialization function, if present.
     */
    if !aggserialfnName.is_empty() {
        /* signature is always serialize(internal) returns bytea */
        fnArgs[0] = INTERNALOID;

        let (sfn, sret) = lookup_agg_function(&aggserialfnName, 1, &fnArgs, InvalidOid)?;
        serialfn = sfn;
        rettype = sret;

        if rettype != BYTEAOID {
            return Err(datatype_mismatch_return_type(
                "serialization",
                &aggserialfnName,
                BYTEAOID,
            )?);
        }
    }

    /*
     * Validate the deserialization function, if present.
     */
    if !aggdeserialfnName.is_empty() {
        /* signature is always deserialize(bytea, internal) returns internal */
        fnArgs[0] = BYTEAOID;
        fnArgs[1] = INTERNALOID; /* dummy argument for type safety */

        let (dfn, dret) = lookup_agg_function(&aggdeserialfnName, 2, &fnArgs, InvalidOid)?;
        deserialfn = dfn;
        rettype = dret;

        if rettype != INTERNALOID {
            return Err(datatype_mismatch_return_type(
                "deserialization",
                &aggdeserialfnName,
                INTERNALOID,
            )?);
        }
    }

    /*
     * If finaltype (i.e. aggregate return type) is polymorphic, inputs must be
     * polymorphic also, else parser will fail to deduce result type.  (Note:
     * given the previous test on transtype and inputs, this cannot happen, unless
     * someone has snuck a finalfn definition into the catalogs that itself
     * violates the rule against polymorphic result with no polymorphic input.)
     */
    detailmsg = check_valid_polymorphic_signature::call(finaltype, aggArgTypes, numArgs)?;
    if let Some(detailmsg) = detailmsg {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_DATATYPE_MISMATCH)
            .errmsg("cannot determine result data type")
            .errdetail_internal(detailmsg)
            .into_error());
    }

    /*
     * Also, the return type can't be INTERNAL unless there's at least one INTERNAL
     * argument.  This is the same type-safety restriction we enforce for regular
     * functions, but at the level of aggregates.  We must test this explicitly
     * because we allow INTERNAL as the transtype.
     */
    detailmsg = check_valid_internal_signature::call(finaltype, aggArgTypes, numArgs)?;
    if let Some(detailmsg) = detailmsg {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
            .errmsg("unsafe use of pseudo-type \"internal\"")
            .errdetail_internal(detailmsg)
            .into_error());
    }

    /*
     * If a moving-aggregate implementation is supplied, look up its finalfn if
     * any, and check that the implied aggregate result type matches the plain
     * implementation.
     */
    if OidIsValid(aggmTransType) {
        /* handle finalfn, if supplied */
        if !aggmfinalfnName.is_empty() {
            /*
             * The arguments are figured the same way as for the regular finalfn,
             * but using aggmTransType and mfinalfnExtraArgs.
             */
            let mut ffnVariadicArgType = variadicArgType;

            fnArgs[0] = aggmTransType;
            fnArgs[1..1 + numArgs as usize].copy_from_slice(&aggArgTypes[..numArgs as usize]);
            if mfinalfnExtraArgs {
                nargs_finalfn = numArgs + 1;
            } else {
                nargs_finalfn = numDirectArgs + 1;
                if numDirectArgs < numArgs {
                    /* variadic argument doesn't affect finalfn */
                    ffnVariadicArgType = InvalidOid;
                }
            }

            let (mffn, mfret) =
                lookup_agg_function(&aggmfinalfnName, nargs_finalfn, &fnArgs, ffnVariadicArgType)?;
            mfinalfn = mffn;
            rettype = mfret;

            /* As above, check strictness if mfinalfnExtraArgs is given */
            if mfinalfnExtraArgs && func_strict::call(mfinalfn)? {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
                    .errmsg("final function with extra arguments must not be declared STRICT")
                    .into_error());
            }
        } else {
            /*
             * If no finalfn, aggregate result type is type of the state value
             */
            rettype = aggmTransType;
        }
        debug_assert!(OidIsValid(rettype));
        if rettype != finaltype {
            let ctx = MemoryContext::new("AggregateCreate magg result");
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
                .errmsg(format!(
                    "moving-aggregate implementation returns type {}, but plain implementation returns type {}",
                    format_type_be::call(ctx.mcx(), rettype)?.as_str(),
                    format_type_be::call(ctx.mcx(), finaltype)?.as_str()
                ))
                .into_error());
        }
    }

    /* handle sortop, if supplied */
    if !aggsortopName.is_empty() {
        if numArgs != 1 {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
                .errmsg("sort operator can only be specified for single-argument aggregates")
                .into_error());
        }
        /* LookupOperName(NULL, aggsortopName, aggArgTypes[0], aggArgTypes[0], false, -1) */
        sortop = lookup_oper_name::call(&name_list(&aggsortopName), aggArgTypes[0], aggArgTypes[0])?;
    }

    /*
     * permission checks on used types
     */
    let userId = get_user_id::call()?;
    for i in 0..numArgs as usize {
        aclresult = object_aclcheck::call(TypeRelationId, aggArgTypes[i], userId, ACL_USAGE)?;
        if aclresult != ACLCHECK_OK {
            aclcheck_error_type::call(aclresult, aggArgTypes[i])?;
        }
    }

    aclresult = object_aclcheck::call(TypeRelationId, aggTransType, userId, ACL_USAGE)?;
    if aclresult != ACLCHECK_OK {
        aclcheck_error_type::call(aclresult, aggTransType)?;
    }

    if OidIsValid(aggmTransType) {
        aclresult = object_aclcheck::call(TypeRelationId, aggmTransType, userId, ACL_USAGE)?;
        if aclresult != ACLCHECK_OK {
            aclcheck_error_type::call(aclresult, aggmTransType)?;
        }
    }

    aclresult = object_aclcheck::call(TypeRelationId, finaltype, userId, ACL_USAGE)?;
    if aclresult != ACLCHECK_OK {
        aclcheck_error_type::call(aclresult, finaltype)?;
    }

    /*
     * Everything looks okay.  Try to create the pg_proc entry for the aggregate.
     * (This could fail if there's already a conflicting entry.)
     */
    let myself: ObjectAddress = procedure_create::call(ProcedureCreateArgs {
        procedure_name: aggName.clone(),
        namespace_id: aggNamespace,
        replace,                       /* maybe replacement */
        returns_set: false,            /* doesn't return a set */
        prorettype: finaltype,         /* returnType */
        proowner: userId,              /* proowner = GetUserId() */
        language_oid: INTERNALlanguageId, /* languageObjectId */
        language_validator: InvalidOid, /* no validator */
        prosrc: String::from("aggregate_dummy"), /* placeholder (no such proc) */
        probin: None,                  /* probin */
        prosqlbody: None,              /* prosqlbody */
        prosqlbody_refs: alloc::vec::Vec::new(), /* prosqlbody_refs */
        prokind: PROKIND_AGGREGATE,
        security: false, /* security invoker (currently not definable for agg) */
        is_leak_proof: false, /* isLeakProof */
        is_strict: false, /* isStrict (not needed for agg) */
        volatility: PROVOLATILE_IMMUTABLE, /* volatility (not needed for agg) */
        parallel: proparallel,
        parameter_types: parameterTypes.clone(), /* paramTypes */
        all_parameter_types: allParameterTypes,   /* allParamTypes */
        parameter_modes: parameterModes,          /* parameterModes */
        parameter_names: parameterNames,          /* parameterNames */
        parameter_defaults: parameterDefaults,    /* parameterDefaults */
        trftypes: None,                           /* trftypes = PointerGetDatum(NULL) */
        trfoids: Vec::new(),                      /* trfoids = NIL */
        proconfig: None,                          /* proconfig = PointerGetDatum(NULL) */
        prosupport: InvalidOid,                   /* no prosupport */
        procost: 1.0,                             /* procost */
        prorows: 0.0,                             /* prorows */
    })?;
    let procOid: Oid = myself.objectId;

    /*
     * Okay to create the pg_aggregate entry.
     */
    let ctx = MemoryContext::new("AggregateCreate");
    let mcx = ctx.mcx();
    let aggdesc = table_open(mcx, AggregateRelationId, RowExclusiveLock)?;

    /*
     * Build the row.  In C every column gets values[i], nulls[i] = false,
     * replaces[i] = true (then the specific columns are set); all fixed-length
     * columns are always written (never NULL), and only agginitval / aggminitval
     * can be NULL.  The fixed-length prefix is the typed FormData_pg_aggregate;
     * the two trailing text columns are the Options.
     */
    let row = PgAggregateInsertRow {
        form: FormData_pg_aggregate {
            aggfnoid: procOid,
            aggkind: aggKind,
            aggnumdirectargs: numDirectArgs as i16,
            aggtransfn: transfn,
            aggfinalfn: finalfn,
            aggcombinefn: combinefn,
            aggserialfn: serialfn,
            aggdeserialfn: deserialfn,
            aggmtransfn: mtransfn,
            aggminvtransfn: minvtransfn,
            aggmfinalfn: mfinalfn,
            aggfinalextra: finalfnExtraArgs,
            aggmfinalextra: mfinalfnExtraArgs,
            aggfinalmodify: finalfnModify,
            aggmfinalmodify: mfinalfnModify,
            aggsortop: sortop,
            aggtranstype: aggTransType,
            aggtransspace: aggTransSpace,
            aggmtranstype: aggmTransType,
            aggmtransspace: aggmTransSpace,
        },
        agginitval: agginitval.clone(),
        aggminitval: aggminitval.clone(),
    };

    /*
     * if (replace) oldtup = SearchSysCache1(AGGFNOID, ObjectIdGetDatum(procOid));
     * else oldtup = NULL;
     */
    let oldtup = if replace {
        aggregate_tuple_by_fnoid::call(mcx, procOid)?
    } else {
        None
    };

    if let Some((oldtuple, oldagg)) = oldtup {
        /*
         * If we're replacing an existing entry, we need to validate that we're
         * not changing anything that would break callers. Specifically we must
         * not change aggkind or aggnumdirectargs, which affect how an aggregate
         * call is treated in parse analysis.
         */
        if aggKind != oldagg.aggkind {
            let mut b = ereport(ERROR)
                .errcode(ERRCODE_WRONG_OBJECT_TYPE)
                .errmsg("cannot change routine kind");
            if oldagg.aggkind == AGGKIND_NORMAL {
                b = b.errdetail(format!("\"{aggName}\" is an ordinary aggregate function."));
            } else if oldagg.aggkind == AGGKIND_ORDERED_SET {
                b = b.errdetail(format!("\"{aggName}\" is an ordered-set aggregate."));
            } else if oldagg.aggkind == AGGKIND_HYPOTHETICAL {
                b = b.errdetail(format!("\"{aggName}\" is a hypothetical-set aggregate."));
            }
            return Err(b.into_error());
        }
        if numDirectArgs != oldagg.aggnumdirectargs {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
                .errmsg("cannot change number of direct arguments of an aggregate function")
                .into_error());
        }

        /*
         * replaces[Anum_pg_aggregate_aggfnoid - 1] = false;
         * replaces[Anum_pg_aggregate_aggkind - 1] = false;
         * replaces[Anum_pg_aggregate_aggnumdirectargs - 1] = false;
         */
        let replaces = PgAggregateReplaces {
            aggfnoid: false,
            aggkind: false,
            aggnumdirectargs: false,
        };

        /*
         * tup = heap_modify_tuple(oldtup, tupDesc, values, nulls, replaces);
         * CatalogTupleUpdate(aggdesc, &tup->t_self, tup);
         * ReleaseSysCache(oldtup);
         */
        indexing_seams::catalog_tuple_update_pg_aggregate::call(
            mcx, &aggdesc, &oldtuple, &row, replaces,
        )?;
    } else {
        /*
         * tup = heap_form_tuple(tupDesc, values, nulls);
         * CatalogTupleInsert(aggdesc, tup);
         */
        indexing_seams::catalog_tuple_insert_pg_aggregate::call(mcx, &aggdesc, &row)?;
    }

    aggdesc.close(RowExclusiveLock)?;

    /*
     * Create dependencies for the aggregate (above and beyond those already made
     * by ProcedureCreate).  Note: we don't need an explicit dependency on
     * aggTransType since we depend on it indirectly through transfn.  Likewise for
     * aggmTransType using the mtransfn, if it exists.
     *
     * If we're replacing an existing definition, ProcedureCreate deleted all our
     * existing dependencies, so we have to do the same things here either way.
     */
    let mut addrs = new_object_addresses();

    /* Depends on transition function */
    referenced = ObjectAddressSet(ProcedureRelationId, transfn);
    add_exact_object_address(&referenced, &mut addrs);

    /* Depends on final function, if any */
    if OidIsValid(finalfn) {
        referenced = ObjectAddressSet(ProcedureRelationId, finalfn);
        add_exact_object_address(&referenced, &mut addrs);
    }

    /* Depends on combine function, if any */
    if OidIsValid(combinefn) {
        referenced = ObjectAddressSet(ProcedureRelationId, combinefn);
        add_exact_object_address(&referenced, &mut addrs);
    }

    /* Depends on serialization function, if any */
    if OidIsValid(serialfn) {
        referenced = ObjectAddressSet(ProcedureRelationId, serialfn);
        add_exact_object_address(&referenced, &mut addrs);
    }

    /* Depends on deserialization function, if any */
    if OidIsValid(deserialfn) {
        referenced = ObjectAddressSet(ProcedureRelationId, deserialfn);
        add_exact_object_address(&referenced, &mut addrs);
    }

    /* Depends on forward transition function, if any */
    if OidIsValid(mtransfn) {
        referenced = ObjectAddressSet(ProcedureRelationId, mtransfn);
        add_exact_object_address(&referenced, &mut addrs);
    }

    /* Depends on inverse transition function, if any */
    if OidIsValid(minvtransfn) {
        referenced = ObjectAddressSet(ProcedureRelationId, minvtransfn);
        add_exact_object_address(&referenced, &mut addrs);
    }

    /* Depends on final function, if any */
    if OidIsValid(mfinalfn) {
        referenced = ObjectAddressSet(ProcedureRelationId, mfinalfn);
        add_exact_object_address(&referenced, &mut addrs);
    }

    /* Depends on sort operator, if any */
    if OidIsValid(sortop) {
        referenced = ObjectAddressSet(OperatorRelationId, sortop);
        add_exact_object_address(&referenced, &mut addrs);
    }

    /* record_object_address_dependencies(&myself, addrs, DEPENDENCY_NORMAL); */
    let behavior: DependencyType = DEPENDENCY_NORMAL;
    record_object_address_dependencies(&myself, &mut addrs, behavior)?;
    /* free_object_addresses(addrs): the owned ObjectAddresses Vec is dropped. */
    drop(addrs);

    Ok(myself)
}

/// `errmsg("return type of <kind> function %s is not %s")` with
/// `ERRCODE_DATATYPE_MISMATCH` — the shared shape of the transfn / mtransfn /
/// minvtransfn / combinefn / serialfn / deserialfn return-type checks.
fn datatype_mismatch_return_type(
    kind: &str,
    fnName: &[parsenodes::Node],
    wanted_type: Oid,
) -> PgResult<PgError> {
    let ctx = MemoryContext::new("AggregateCreate rettype mismatch");
    Ok(ereport(ERROR)
        .errcode(ERRCODE_DATATYPE_MISMATCH)
        .errmsg(format!(
            "return type of {} function {} is not {}",
            kind,
            name_list_to_string::call(name_list(fnName))?,
            format_type_be::call(ctx.mcx(), wanted_type)?.as_str()
        ))
        .into_error())
}

/* ===========================================================================
 * lookup_agg_function (pg_aggregate.c:826-915)
 * ========================================================================= */

/// `lookup_agg_function` — common code for finding aggregate support functions.
///
/// `fnName`: possibly-schema-qualified function name list.
/// `nargs`, `input_types`: expected function argument types.
/// `variadicArgType`: type of variadic argument if any, else `InvalidOid`.
///
/// Returns OID of function and its return type (the C `*rettype` out-param, here
/// the second tuple element).
///
/// NB: must not scribble on `input_types[]`, as we may re-use those.
fn lookup_agg_function(
    fnName: &[parsenodes::Node],
    nargs: i32,
    input_types: &[Oid],
    variadicArgType: Oid,
) -> PgResult<(Oid, Oid)> {
    let ctx = MemoryContext::new("lookup_agg_function");
    let mcx = ctx.mcx();

    let fn_name_strs = name_list(fnName);
    /* func_get_detail uses only the first `nargs` entries of input_types. */
    let in_types = &input_types[..nargs as usize];

    /*
     * func_get_detail looks up the function in the catalogs, does disambiguation
     * for polymorphic functions, handles inheritance, and returns the funcid and
     * type and set or singleton status of the function's return value.  it also
     * returns the true argument types to the function.
     */
    let detail = func_get_detail::call(mcx, &fn_name_strs, &[], nargs, in_types, false, false)?;
    let fnOid = detail.funcid;
    let mut rettype = detail.rettype;
    let retset = detail.retset;
    let vatype = detail.vatype;
    /* func_get_detail will find functions requiring run-time argument type
     * coercion; the true declared argument types come back here. */
    let mut true_oid_array: Vec<Oid> = detail.true_typeids.iter().copied().collect();

    /* only valid case is a normal function not returning a set */
    if detail.fdresult != FuncDetailCode::Normal || !OidIsValid(fnOid) {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_FUNCTION)
            .errmsg(format!(
                "function {} does not exist",
                func_signature_string::call(fn_name_strs.clone(), nargs, in_types.to_vec())?
            ))
            .into_error());
    }
    if retset {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_DATATYPE_MISMATCH)
            .errmsg(format!(
                "function {} returns a set",
                func_signature_string::call(fn_name_strs.clone(), nargs, in_types.to_vec())?
            ))
            .into_error());
    }

    /*
     * If the agg is declared to take VARIADIC ANY, the underlying functions had
     * better be declared that way too, else they may receive too many parameters;
     * but func_get_detail would have been happy with plain ANY.  (Probably nothing
     * very bad would happen, but it wouldn't work as the user expects.)  Other
     * combinations should work without any special pushups, given that we told
     * func_get_detail not to expand VARIADIC.
     */
    if variadicArgType == ANYOID && vatype != ANYOID {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_DATATYPE_MISMATCH)
            .errmsg(format!(
                "function {} must accept VARIADIC ANY to be used in this aggregate",
                func_signature_string::call(fn_name_strs.clone(), nargs, in_types.to_vec())?
            ))
            .into_error());
    }

    /*
     * If there are any polymorphic types involved, enforce consistency, and
     * possibly refine the result type.  It's OK if the result is still polymorphic
     * at this point, though.
     */
    rettype = enforce_generic_type_consistency::call(
        in_types,
        &mut true_oid_array,
        nargs,
        rettype,
        true,
    )?;

    /*
     * func_get_detail will find functions requiring run-time argument type
     * coercion, but nodeAgg.c isn't prepared to deal with that
     */
    for i in 0..nargs as usize {
        if !is_binary_coercible::call(in_types[i], true_oid_array[i])? {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_DATATYPE_MISMATCH)
                .errmsg(format!(
                    "function {} requires run-time type coercion",
                    func_signature_string::call(
                        fn_name_strs.clone(),
                        nargs,
                        true_oid_array.clone()
                    )?
                ))
                .into_error());
        }
    }

    /* Check aggregate creator has permission to call the function */
    let aclresult = object_aclcheck::call(ProcedureRelationId, fnOid, get_user_id::call()?, ACL_EXECUTE)?;
    if aclresult != ACLCHECK_OK {
        /* aclcheck_error(aclresult, OBJECT_FUNCTION, get_func_name(fnOid)) */
        let name = get_func_name::call(mcx, fnOid)?
            .map(|s| s.as_str().to_string())
            .unwrap_or_default();
        aclcheck_error_function::call(aclresult, name)?;
    }

    Ok((fnOid, rettype))
}

/// Install this unit's inward seam ([`pg_aggregate_seams`]).
/// `aggregatecmds.c`'s `DefineAggregate` consumes `aggregate_create`.
pub fn init_seams() {
    ::pg_aggregate_seams::aggregate_create::set(AggregateCreate);
}
