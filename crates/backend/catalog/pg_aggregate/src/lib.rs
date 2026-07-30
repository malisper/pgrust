//! pg_aggregate.c — AggregateCreate and support-function resolution.
#![allow(non_snake_case, non_upper_case_globals)]

use cache_syscache::{SearchSysCacheCopy, SysCacheKey, AGGFNOID};
use catalog_namespace::FuncnameGetCandidates;
use datum::Datum;
use mcx::Mcx;
use pg_depend::{DependencyType, ObjectAddress};
use pg_proc::{
    check_valid_internal_signature, check_valid_polymorphic_signature, ProcedureCreate,
    ProcedureCreateArgs, FUNC_MAX_ARGS, INTERNALlanguageId, PROKIND_AGGREGATE,
    PROVOLATILE_IMMUTABLE,
};
use types_core::{
    InvalidOid, Oid, OidIsValid, AGGREGATE_RELATION_ID, ANYOID, BYTEAOID, INTERNALOID,
    OPERATOR_RELATION_ID, PROCEDURE_RELATION_ID, TYPE_RELATION_ID,
};
use types_error::{
    PgError, PgResult, ERRCODE_DATATYPE_MISMATCH, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INVALID_FUNCTION_DEFINITION, ERRCODE_TOO_MANY_ARGUMENTS, ERRCODE_UNDEFINED_FUNCTION,
    ERRCODE_WRONG_OBJECT_TYPE,
};
use types_nodes::parsenodes::ObjectType;
use types_nodes::NodeList;
use types_rel::RowExclusiveLock;

pub const Natts_pg_aggregate: usize = 22;
pub const Anum_pg_aggregate_aggfnoid: i32 = 1;
pub const Anum_pg_aggregate_aggkind: i32 = 2;
pub const Anum_pg_aggregate_aggnumdirectargs: i32 = 3;
pub const Anum_pg_aggregate_aggtransfn: i32 = 4;
pub const Anum_pg_aggregate_aggfinalfn: i32 = 5;
pub const Anum_pg_aggregate_aggcombinefn: i32 = 6;
pub const Anum_pg_aggregate_aggserialfn: i32 = 7;
pub const Anum_pg_aggregate_aggdeserialfn: i32 = 8;
pub const Anum_pg_aggregate_aggmtransfn: i32 = 9;
pub const Anum_pg_aggregate_aggminvtransfn: i32 = 10;
pub const Anum_pg_aggregate_aggmfinalfn: i32 = 11;
pub const Anum_pg_aggregate_aggfinalextra: i32 = 12;
pub const Anum_pg_aggregate_aggmfinalextra: i32 = 13;
pub const Anum_pg_aggregate_aggfinalmodify: i32 = 14;
pub const Anum_pg_aggregate_aggmfinalmodify: i32 = 15;
pub const Anum_pg_aggregate_aggsortop: i32 = 16;
pub const Anum_pg_aggregate_aggtranstype: i32 = 17;
pub const Anum_pg_aggregate_aggtransspace: i32 = 18;
pub const Anum_pg_aggregate_aggmtranstype: i32 = 19;
pub const Anum_pg_aggregate_aggmtransspace: i32 = 20;
pub const Anum_pg_aggregate_agginitval: i32 = 21;
pub const Anum_pg_aggregate_aggminitval: i32 = 22;

pub const AGGKIND_NORMAL: i8 = b'n' as i8;
pub const AGGKIND_ORDERED_SET: i8 = b'o' as i8;
pub const AGGKIND_HYPOTHETICAL: i8 = b'h' as i8;

pub const AGGMODIFY_READ_ONLY: i8 = b'r' as i8;
pub const AGGMODIFY_SHAREABLE: i8 = b's' as i8;
pub const AGGMODIFY_READ_WRITE: i8 = b'w' as i8;

pub fn AGGKIND_IS_ORDERED_SET(kind: i8) -> bool {
    kind != AGGKIND_NORMAL
}

#[track_caller]
#[cold]
fn err(sqlstate: types_error::SqlState, msg: String) -> Box<PgError> {
    Box::new(PgError::error(msg).with_sqlstate(sqlstate))
}

pub struct AggregateCreateArgs<'a, 'mcx> {
    pub agg_name: &'a str,
    pub agg_namespace: Oid,
    pub replace: bool,
    pub agg_kind: i8,
    pub num_direct_args: i32,
    pub parameter_types: &'a [Oid],
    pub all_parameter_types: Option<&'a [Oid]>,
    pub parameter_modes: Option<&'a [i8]>,
    pub parameter_names: Option<&'a [&'a str]>,
    pub variadic_arg_type: Oid,
    pub transfn_name: &'a NodeList<'mcx>,
    pub finalfn_name: Option<&'a NodeList<'mcx>>,
    pub combinefn_name: Option<&'a NodeList<'mcx>>,
    pub serialfn_name: Option<&'a NodeList<'mcx>>,
    pub deserialfn_name: Option<&'a NodeList<'mcx>>,
    pub mtransfn_name: Option<&'a NodeList<'mcx>>,
    pub minvtransfn_name: Option<&'a NodeList<'mcx>>,
    pub mfinalfn_name: Option<&'a NodeList<'mcx>>,
    pub finalfn_extra_args: bool,
    pub mfinalfn_extra_args: bool,
    pub finalfn_modify: i8,
    pub mfinalfn_modify: i8,
    pub sortop_name: Option<&'a NodeList<'mcx>>,
    pub agg_trans_type: Oid,
    pub agg_trans_space: i32,
    pub agg_mtrans_type: Oid,
    pub agg_mtrans_space: i32,
    pub agg_initval: Option<&'a str>,
    pub agg_minitval: Option<&'a str>,
    pub proparallel: i8,
}

fn type_acl_check(typeId: Oid) -> PgResult<()> {
    let aclresult = aclchk::object_aclcheck(
        TYPE_RELATION_ID,
        typeId,
        miscinit::GetUserId(),
        adt_acl::ACL_USAGE,
    )?;
    if aclresult != aclchk::ACLCHECK_OK {
        // aclcheck_error_type
        let name = format_type::format_type_be(typeId)?;
        aclchk::aclcheck_error(aclresult, ObjectType::OBJECT_TYPE, &name)?;
    }
    Ok(())
}

#[track_caller]
#[cold]
fn detail_error(msg: &str, sqlstate: types_error::SqlState, detail: String) -> Box<PgError> {
    Box::new(PgError::error(msg.to_string()).with_sqlstate(sqlstate).with_detail(detail))
}

pub fn AggregateCreate<'mcx>(
    mcx: Mcx<'mcx>,
    a: &AggregateCreateArgs<'_, 'mcx>,
) -> PgResult<ObjectAddress> {
    let numArgs = a.parameter_types.len() as i32;
    let numDirectArgs = a.num_direct_args;
    let aggArgTypes = a.parameter_types;
    let mut fnArgs = [InvalidOid; FUNC_MAX_ARGS];

    if a.agg_name.is_empty() {
        panic!("no aggregate name supplied");
    }
    if a.transfn_name.is_nil() {
        panic!("aggregate must have a transition function");
    }
    if numDirectArgs < 0 || numDirectArgs > numArgs {
        panic!("incorrect number of direct arguments for aggregate");
    }
    if numArgs > FUNC_MAX_ARGS as i32 - 1 {
        return Err(err(
            ERRCODE_TOO_MANY_ARGUMENTS,
            format!("aggregates cannot have more than {} arguments", FUNC_MAX_ARGS - 1),
        ));
    }

    if let Some(detail) = check_valid_polymorphic_signature(a.agg_trans_type, aggArgTypes)? {
        return Err(detail_error(
            "cannot determine transition data type",
            ERRCODE_INVALID_FUNCTION_DEFINITION,
            detail,
        ));
    }
    if OidIsValid(a.agg_mtrans_type) {
        if let Some(detail) = check_valid_polymorphic_signature(a.agg_mtrans_type, aggArgTypes)? {
            return Err(detail_error(
                "cannot determine transition data type",
                ERRCODE_INVALID_FUNCTION_DEFINITION,
                detail,
            ));
        }
    }

    if AGGKIND_IS_ORDERED_SET(a.agg_kind)
        && OidIsValid(a.variadic_arg_type)
        && a.variadic_arg_type != ANYOID
    {
        return Err(err(
            ERRCODE_FEATURE_NOT_SUPPORTED,
            "a variadic ordered-set aggregate must use VARIADIC type ANY".to_string(),
        ));
    }

    if a.agg_kind == AGGKIND_HYPOTHETICAL && numDirectArgs < numArgs {
        let numAggregatedArgs = (numArgs - numDirectArgs) as usize;
        let d = numDirectArgs as usize;
        if OidIsValid(a.variadic_arg_type)
            || numDirectArgs < numAggregatedArgs as i32
            || aggArgTypes[d - numAggregatedArgs..d] != aggArgTypes[d..]
        {
            return Err(err(
                ERRCODE_INVALID_FUNCTION_DEFINITION,
                "a hypothetical-set aggregate must have direct arguments matching its aggregated arguments".to_string(),
            ));
        }
    }

    // For ordinary aggs the transfn takes the transtype plus all arguments;
    // for ordered-set aggs, the transtype plus aggregated args only, except a
    // trailing VARIADIC covers both sides.
    let nargs_transfn;
    if AGGKIND_IS_ORDERED_SET(a.agg_kind) {
        if numDirectArgs < numArgs {
            nargs_transfn = (numArgs - numDirectArgs + 1) as usize;
        } else {
            debug_assert!(a.variadic_arg_type != InvalidOid);
            nargs_transfn = 2;
        }
        fnArgs[0] = a.agg_trans_type;
        let start = (numArgs as usize) - (nargs_transfn - 1);
        fnArgs[1..nargs_transfn].copy_from_slice(&aggArgTypes[start..]);
    } else {
        nargs_transfn = numArgs as usize + 1;
        fnArgs[0] = a.agg_trans_type;
        fnArgs[1..nargs_transfn].copy_from_slice(aggArgTypes);
    }
    let (transfn, rettype) = lookup_agg_function(
        mcx,
        a.transfn_name,
        nargs_transfn,
        &fnArgs[..nargs_transfn],
        a.variadic_arg_type,
    )?;

    // Transfn return type must exactly match the declared transtype.
    if rettype != a.agg_trans_type {
        return Err(err(
            ERRCODE_DATATYPE_MISMATCH,
            format!(
                "return type of transition function {} is not {}",
                commands_define::NameListToString(mcx, a.transfn_name)?.as_str(),
                format_type::format_type_be(a.agg_trans_type)?
            ),
        ));
    }

    let proc_shape = |fnoid: Oid| {
        syscache_seams::lookup_pg_proc_shape::call(fnoid)
            .map(|s| s.unwrap_or_else(|| panic!("cache lookup failed for function {fnoid}")))
    };

    // A strict transfn with NULL initval seeds the state from the first
    // input, so that input must be binary-compatible with the transtype.
    let proc = proc_shape(transfn)?;
    if proc.proisstrict && a.agg_initval.is_none() {
        if numArgs < 1 || !coerce::IsBinaryCoercible(aggArgTypes[0], a.agg_trans_type)? {
            return Err(err(
                ERRCODE_INVALID_FUNCTION_DEFINITION,
                "must not omit initial value when transition function is strict and transition type is not compatible with input type".to_string(),
            ));
        }
    }

    let mut mtransfn = InvalidOid;
    let mut minvtransfn = InvalidOid;
    let mut mtransIsStrict = false;
    if let Some(mtransfn_name) = a.mtransfn_name {
        // Same args as the regular transfn except the transition type.
        debug_assert!(OidIsValid(a.agg_mtrans_type));
        fnArgs[0] = a.agg_mtrans_type;

        let (f, rettype) = lookup_agg_function(
            mcx,
            mtransfn_name,
            nargs_transfn,
            &fnArgs[..nargs_transfn],
            a.variadic_arg_type,
        )?;
        mtransfn = f;

        if rettype != a.agg_mtrans_type {
            return Err(err(
                ERRCODE_DATATYPE_MISMATCH,
                format!(
                    "return type of transition function {} is not {}",
                    commands_define::NameListToString(mcx, mtransfn_name)?.as_str(),
                    format_type::format_type_be(a.agg_mtrans_type)?
                ),
            ));
        }

        let proc = proc_shape(mtransfn)?;
        if proc.proisstrict && a.agg_minitval.is_none() {
            if numArgs < 1 || !coerce::IsBinaryCoercible(aggArgTypes[0], a.agg_mtrans_type)? {
                return Err(err(
                    ERRCODE_INVALID_FUNCTION_DEFINITION,
                    "must not omit initial value when transition function is strict and transition type is not compatible with input type".to_string(),
                ));
            }
        }
        mtransIsStrict = proc.proisstrict;
    }

    if let Some(minvtransfn_name) = a.minvtransfn_name {
        debug_assert!(a.mtransfn_name.is_some());
        let (f, rettype) = lookup_agg_function(
            mcx,
            minvtransfn_name,
            nargs_transfn,
            &fnArgs[..nargs_transfn],
            a.variadic_arg_type,
        )?;
        minvtransfn = f;

        if rettype != a.agg_mtrans_type {
            return Err(err(
                ERRCODE_DATATYPE_MISMATCH,
                format!(
                    "return type of inverse transition function {} is not {}",
                    commands_define::NameListToString(mcx, minvtransfn_name)?.as_str(),
                    format_type::format_type_be(a.agg_mtrans_type)?
                ),
            ));
        }

        // Forward and inverse strictness must agree (simplifies execution).
        let proc = proc_shape(minvtransfn)?;
        if proc.proisstrict != mtransIsStrict {
            return Err(err(
                ERRCODE_INVALID_FUNCTION_DEFINITION,
                "strictness of aggregate's forward and inverse transition functions must match".to_string(),
            ));
        }
    }

    let mut finalfn = InvalidOid;
    let finaltype;
    if let Some(finalfn_name) = a.finalfn_name {
        // With finalfn_extra the finalfn takes the transtype plus all args
        // (passed as NULLs at runtime, but needed to resolve a polymorphic
        // agg's result type); otherwise transtype plus direct args only.
        let mut ffnVariadicArgType = a.variadic_arg_type;
        fnArgs[0] = a.agg_trans_type;
        fnArgs[1..=numArgs as usize].copy_from_slice(aggArgTypes);
        let nargs_finalfn;
        if a.finalfn_extra_args {
            nargs_finalfn = numArgs as usize + 1;
        } else {
            nargs_finalfn = numDirectArgs as usize + 1;
            if numDirectArgs < numArgs {
                // variadic argument doesn't affect finalfn
                ffnVariadicArgType = InvalidOid;
            }
        }

        let (f, ft) = lookup_agg_function(
            mcx,
            finalfn_name,
            nargs_finalfn,
            &fnArgs[..nargs_finalfn],
            ffnVariadicArgType,
        )?;
        finalfn = f;
        finaltype = ft;

        // finalfn_extra guarantees at least one null argument at runtime.
        if a.finalfn_extra_args && lsyscache::func_strict(finalfn)? {
            return Err(err(
                ERRCODE_INVALID_FUNCTION_DEFINITION,
                "final function with extra arguments must not be declared STRICT".to_string(),
            ));
        }
    } else {
        // No finalfn: result type is the state value's type.
        finaltype = a.agg_trans_type;
    }
    debug_assert!(OidIsValid(finaltype));

    let mut combinefn = InvalidOid;
    if let Some(combinefn_name) = a.combinefn_name {
        // combine(transtype, transtype) -> transtype; VARIADIC irrelevant.
        fnArgs[0] = a.agg_trans_type;
        fnArgs[1] = a.agg_trans_type;
        let (f, combineType) =
            lookup_agg_function(mcx, combinefn_name, 2, &fnArgs[..2], InvalidOid)?;
        combinefn = f;

        if combineType != a.agg_trans_type {
            return Err(err(
                ERRCODE_DATATYPE_MISMATCH,
                format!(
                    "return type of combine function {} is not {}",
                    commands_define::NameListToString(mcx, combinefn_name)?.as_str(),
                    format_type::format_type_be(a.agg_trans_type)?
                ),
            ));
        }

        // An INTERNAL-state combine function must accept nulls.
        if a.agg_trans_type == INTERNALOID && lsyscache::func_strict(combinefn)? {
            return Err(err(
                ERRCODE_INVALID_FUNCTION_DEFINITION,
                format!(
                    "combine function with transition type {} must not be declared STRICT",
                    format_type::format_type_be(a.agg_trans_type)?
                ),
            ));
        }
    }

    let mut serialfn = InvalidOid;
    if let Some(serialfn_name) = a.serialfn_name {
        // serialize(internal) returns bytea
        fnArgs[0] = INTERNALOID;
        let (f, rettype) = lookup_agg_function(mcx, serialfn_name, 1, &fnArgs[..1], InvalidOid)?;
        serialfn = f;
        if rettype != BYTEAOID {
            return Err(err(
                ERRCODE_DATATYPE_MISMATCH,
                format!(
                    "return type of serialization function {} is not {}",
                    commands_define::NameListToString(mcx, serialfn_name)?.as_str(),
                    format_type::format_type_be(BYTEAOID)?
                ),
            ));
        }
    }

    let mut deserialfn = InvalidOid;
    if let Some(deserialfn_name) = a.deserialfn_name {
        // deserialize(bytea, internal) returns internal
        fnArgs[0] = BYTEAOID;
        fnArgs[1] = INTERNALOID;
        let (f, rettype) = lookup_agg_function(mcx, deserialfn_name, 2, &fnArgs[..2], InvalidOid)?;
        deserialfn = f;
        if rettype != INTERNALOID {
            return Err(err(
                ERRCODE_DATATYPE_MISMATCH,
                format!(
                    "return type of deserialization function {} is not {}",
                    commands_define::NameListToString(mcx, deserialfn_name)?.as_str(),
                    format_type::format_type_be(INTERNALOID)?
                ),
            ));
        }
    }

    if let Some(detail) = check_valid_polymorphic_signature(finaltype, aggArgTypes)? {
        return Err(detail_error(
            "cannot determine result data type",
            ERRCODE_DATATYPE_MISMATCH,
            detail,
        ));
    }
    if let Some(detail) = check_valid_internal_signature(finaltype, aggArgTypes) {
        return Err(detail_error(
            "unsafe use of pseudo-type \"internal\"",
            ERRCODE_INVALID_FUNCTION_DEFINITION,
            detail.to_string(),
        ));
    }

    let mut mfinalfn = InvalidOid;
    if OidIsValid(a.agg_mtrans_type) {
        let rettype;
        if let Some(mfinalfn_name) = a.mfinalfn_name {
            let mut ffnVariadicArgType = a.variadic_arg_type;
            fnArgs[0] = a.agg_mtrans_type;
            fnArgs[1..=numArgs as usize].copy_from_slice(aggArgTypes);
            let nargs_finalfn;
            if a.mfinalfn_extra_args {
                nargs_finalfn = numArgs as usize + 1;
            } else {
                nargs_finalfn = numDirectArgs as usize + 1;
                if numDirectArgs < numArgs {
                    ffnVariadicArgType = InvalidOid;
                }
            }

            let (f, rt) = lookup_agg_function(
                mcx,
                mfinalfn_name,
                nargs_finalfn,
                &fnArgs[..nargs_finalfn],
                ffnVariadicArgType,
            )?;
            mfinalfn = f;
            rettype = rt;

            if a.mfinalfn_extra_args && lsyscache::func_strict(mfinalfn)? {
                return Err(err(
                    ERRCODE_INVALID_FUNCTION_DEFINITION,
                    "final function with extra arguments must not be declared STRICT".to_string(),
                ));
            }
        } else {
            rettype = a.agg_mtrans_type;
        }
        debug_assert!(OidIsValid(rettype));
        if rettype != finaltype {
            return Err(err(
                ERRCODE_INVALID_FUNCTION_DEFINITION,
                format!(
                    "moving-aggregate implementation returns type {}, but plain implementation returns type {}",
                    format_type::format_type_be(rettype)?,
                    format_type::format_type_be(finaltype)?
                ),
            ));
        }
    }

    let mut sortop = InvalidOid;
    if let Some(sortop_name) = a.sortop_name {
        if numArgs != 1 {
            return Err(err(
                ERRCODE_INVALID_FUNCTION_DEFINITION,
                "sort operator can only be specified for single-argument aggregates".to_string(),
            ));
        }
        sortop = parse_oper::LookupOperName(sortop_name, aggArgTypes[0], aggArgTypes[0], false)?;
    }

    for &argtype in aggArgTypes {
        type_acl_check(argtype)?;
    }
    type_acl_check(a.agg_trans_type)?;
    if OidIsValid(a.agg_mtrans_type) {
        type_acl_check(a.agg_mtrans_type)?;
    }
    type_acl_check(finaltype)?;

    let myself = ProcedureCreate(
        mcx,
        &ProcedureCreateArgs {
            procedureName: a.agg_name,
            procNamespace: a.agg_namespace,
            replace: a.replace,
            returnsSet: false,
            returnType: finaltype,
            proowner: miscinit::GetUserId(),
            languageObjectId: INTERNALlanguageId,
            languageValidator: InvalidOid,
            prosrc: "aggregate_dummy", // placeholder (no such proc)
            probin: None,
            prosqlbody: None,
            prokind: PROKIND_AGGREGATE,
            security_definer: false,
            isLeakProof: false,
            isStrict: false,
            volatility: PROVOLATILE_IMMUTABLE,
            parallel: a.proparallel,
            parameterTypes: a.parameter_types,
            allParameterTypes: a.all_parameter_types,
            parameterModes: a.parameter_modes,
            parameterNames: a.parameter_names,
            proconfig: None,
            procost: 1.0,
            prorows: 0.0,
            prosupport: InvalidOid,
            parameterDefaults: None,
            numDefaults: 0,
        },
    )?;
    let procOid = myself.objectId;

    let aggdesc = table::table_open(mcx, AGGREGATE_RELATION_ID, RowExclusiveLock)?;

    let mut values = [Datum::null(); Natts_pg_aggregate];
    let mut nulls = [false; Natts_pg_aggregate];
    let set = |values: &mut [Datum], attnum: i32, d: Datum| values[attnum as usize - 1] = d;
    set(&mut values, Anum_pg_aggregate_aggfnoid, Datum::from_oid(procOid));
    set(&mut values, Anum_pg_aggregate_aggkind, Datum::from_char(a.agg_kind));
    set(&mut values, Anum_pg_aggregate_aggnumdirectargs, Datum::from_i16(numDirectArgs as i16));
    set(&mut values, Anum_pg_aggregate_aggtransfn, Datum::from_oid(transfn));
    set(&mut values, Anum_pg_aggregate_aggfinalfn, Datum::from_oid(finalfn));
    set(&mut values, Anum_pg_aggregate_aggcombinefn, Datum::from_oid(combinefn));
    set(&mut values, Anum_pg_aggregate_aggserialfn, Datum::from_oid(serialfn));
    set(&mut values, Anum_pg_aggregate_aggdeserialfn, Datum::from_oid(deserialfn));
    set(&mut values, Anum_pg_aggregate_aggmtransfn, Datum::from_oid(mtransfn));
    set(&mut values, Anum_pg_aggregate_aggminvtransfn, Datum::from_oid(minvtransfn));
    set(&mut values, Anum_pg_aggregate_aggmfinalfn, Datum::from_oid(mfinalfn));
    set(&mut values, Anum_pg_aggregate_aggfinalextra, Datum::from_bool(a.finalfn_extra_args));
    set(&mut values, Anum_pg_aggregate_aggmfinalextra, Datum::from_bool(a.mfinalfn_extra_args));
    set(&mut values, Anum_pg_aggregate_aggfinalmodify, Datum::from_char(a.finalfn_modify));
    set(&mut values, Anum_pg_aggregate_aggmfinalmodify, Datum::from_char(a.mfinalfn_modify));
    set(&mut values, Anum_pg_aggregate_aggsortop, Datum::from_oid(sortop));
    set(&mut values, Anum_pg_aggregate_aggtranstype, Datum::from_oid(a.agg_trans_type));
    set(&mut values, Anum_pg_aggregate_aggtransspace, Datum::from_i32(a.agg_trans_space));
    set(&mut values, Anum_pg_aggregate_aggmtranstype, Datum::from_oid(a.agg_mtrans_type));
    set(&mut values, Anum_pg_aggregate_aggmtransspace, Datum::from_i32(a.agg_mtrans_space));
    let initval_text = match a.agg_initval {
        Some(s) => Some(varlena::cstring_to_text(mcx, s.as_bytes())?),
        None => None,
    };
    match &initval_text {
        Some(t) => set(
            &mut values,
            Anum_pg_aggregate_agginitval,
            Datum::from_usize(t.as_bytes().as_ptr() as usize),
        ),
        None => nulls[Anum_pg_aggregate_agginitval as usize - 1] = true,
    }
    let minitval_text = match a.agg_minitval {
        Some(s) => Some(varlena::cstring_to_text(mcx, s.as_bytes())?),
        None => None,
    };
    match &minitval_text {
        Some(t) => set(
            &mut values,
            Anum_pg_aggregate_aggminitval,
            Datum::from_usize(t.as_bytes().as_ptr() as usize),
        ),
        None => nulls[Anum_pg_aggregate_aggminitval as usize - 1] = true,
    }

    let oldtup = if a.replace {
        SearchSysCacheCopy(
            mcx,
            AGGFNOID,
            SysCacheKey::Value(Datum::from_oid(procOid)),
            SysCacheKey::UNUSED,
            SysCacheKey::UNUSED,
            SysCacheKey::UNUSED,
        )?
    } else {
        None
    };

    if let Some(oldtup) = oldtup {
        let t = oldtup.as_tuple();
        let desc = aggdesc.descr();
        let mut isnull = false;
        // SAFETY: aggkind/aggnumdirectargs are fixed NOT NULL pg_aggregate columns.
        let old_aggkind =
            unsafe { types_tuple::heap_getattr(t, Anum_pg_aggregate_aggkind, desc, &mut isnull) }
                .as_i8();
        let old_numdirectargs = unsafe {
            types_tuple::heap_getattr(t, Anum_pg_aggregate_aggnumdirectargs, desc, &mut isnull)
        }
        .as_i16();

        // Replacement must not change aggkind or aggnumdirectargs, which
        // affect how an aggregate call is treated in parse analysis.
        if a.agg_kind != old_aggkind {
            let detail = match old_aggkind {
                AGGKIND_NORMAL => {
                    Some(format!("\"{}\" is an ordinary aggregate function.", a.agg_name))
                }
                AGGKIND_ORDERED_SET => {
                    Some(format!("\"{}\" is an ordered-set aggregate.", a.agg_name))
                }
                AGGKIND_HYPOTHETICAL => {
                    Some(format!("\"{}\" is a hypothetical-set aggregate.", a.agg_name))
                }
                _ => None,
            };
            let mut e = PgError::error("cannot change routine kind".to_string())
                .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE);
            if let Some(d) = detail {
                e = e.with_detail(d);
            }
            return Err(Box::new(e));
        }
        if numDirectArgs != old_numdirectargs as i32 {
            return Err(err(
                ERRCODE_INVALID_FUNCTION_DEFINITION,
                "cannot change number of direct arguments of an aggregate function".to_string(),
            ));
        }

        let mut replaces = [true; Natts_pg_aggregate];
        replaces[Anum_pg_aggregate_aggfnoid as usize - 1] = false;
        replaces[Anum_pg_aggregate_aggkind as usize - 1] = false;
        replaces[Anum_pg_aggregate_aggnumdirectargs as usize - 1] = false;

        let mut tup = heaptuple::heap_modify_tuple(mcx, t, desc, &values, &nulls, &replaces)?;
        let otid = t.t_self;
        catalog_indexing::CatalogTupleUpdate(mcx, &aggdesc, &otid, &mut tup)?;
    } else {
        let mut tup = heaptuple::heap_form_tuple(mcx, aggdesc.descr(), &values, &nulls)?;
        catalog_indexing::CatalogTupleInsert(mcx, &aggdesc, &mut tup)?;
    }

    aggdesc.close(RowExclusiveLock)?;

    // Dependencies beyond ProcedureCreate's; the transtypes ride indirectly
    // through the transfns. On replace ProcedureCreate deleted the old
    // records, so both paths record the full set.
    let mut refs = [myself; 9];
    let mut n = 0;
    let mut add = |class_id: Oid, object_id: Oid| {
        if OidIsValid(object_id) {
            refs[n] = ObjectAddress { classId: class_id, objectId: object_id, objectSubId: 0 };
            n += 1;
        }
    };
    add(PROCEDURE_RELATION_ID, transfn);
    add(PROCEDURE_RELATION_ID, finalfn);
    add(PROCEDURE_RELATION_ID, combinefn);
    add(PROCEDURE_RELATION_ID, serialfn);
    add(PROCEDURE_RELATION_ID, deserialfn);
    add(PROCEDURE_RELATION_ID, mtransfn);
    add(PROCEDURE_RELATION_ID, minvtransfn);
    add(PROCEDURE_RELATION_ID, mfinalfn);
    add(OPERATOR_RELATION_ID, sortop);
    let count = n;
    pg_depend::record_object_address_dependencies(
        mcx,
        &myself,
        &mut refs[..count],
        DependencyType::Normal,
    )?;

    Ok(myself)
}

fn func_signature_string(
    mcx: Mcx<'_>,
    fnName: &NodeList<'_>,
    argtypes: &[Oid],
) -> PgResult<String> {
    let mut sig = commands_define::NameListToString(mcx, fnName)?.as_str().to_string();
    sig.push('(');
    for (i, &t) in argtypes.iter().enumerate() {
        if i > 0 {
            sig.push_str(", ");
        }
        sig.push_str(&format_type::format_type_be(t)?);
    }
    sig.push(')');
    Ok(sig)
}

// lookup_agg_function (pg_aggregate.c): resolve one aggregate support
// function via func_get_detail with NIL fargs and no variadic/default
// expansion. Returns (fnOid, rettype); never scribbles on input_types.
fn lookup_agg_function<'mcx>(
    mcx: Mcx<'mcx>,
    fnName: &NodeList<'mcx>,
    nargs: usize,
    input_types: &[Oid],
    variadicArgType: Oid,
) -> PgResult<(Oid, Oid)> {
    let mut buf = [""; 4];
    let parts = name_parts(fnName, &mut buf);

    let candidates = FuncnameGetCandidates(mcx, parts, nargs as i16, &[], false, false)?;

    let mut best: Option<&catalog_namespace::FuncCandidate<'_>> = None;
    for cand in candidates.iter() {
        if cand.args.as_slice() == input_types {
            best = Some(cand);
            break;
        }
    }
    if best.is_none() && !candidates.is_empty() {
        let matched = parse_func::func_match_argtypes(mcx, input_types, candidates.as_slice())?;
        if matched.len() == 1 {
            best = Some(matched[0]);
        } else if matched.len() > 1 {
            // None (FUNCDETAIL_MULTIPLE) falls into the does-not-exist error.
            best = parse_func::func_select_candidate(input_types, matched)?;
        }
    }

    let not_found = |argtypes: &[Oid]| -> PgResult<Box<PgError>> {
        Ok(err(
            ERRCODE_UNDEFINED_FUNCTION,
            format!("function {} does not exist", func_signature_string(mcx, fnName, argtypes)?),
        ))
    };
    let Some(best) = best else {
        return Err(not_found(input_types)?);
    };
    let fnOid = best.oid;
    if !OidIsValid(fnOid) {
        return Err(not_found(input_types)?);
    }
    let shape = syscache_seams::lookup_pg_proc_shape::call(fnOid)?
        .unwrap_or_else(|| panic!("cache lookup failed for function {fnOid}"));
    // Only a plain function yields FUNCDETAIL_NORMAL; any other prokind
    // errors as nonexistent, exactly like C's fdresult check.
    if shape.prokind != b'f' as i8 {
        return Err(not_found(input_types)?);
    }
    if shape.proretset {
        return Err(err(
            ERRCODE_DATATYPE_MISMATCH,
            format!(
                "function {} returns a set",
                func_signature_string(mcx, fnName, input_types)?
            ),
        ));
    }

    // A VARIADIC-ANY agg needs VARIADIC-ANY support functions, else they may
    // receive too many parameters.
    if variadicArgType == ANYOID && shape.provariadic != ANYOID {
        return Err(err(
            ERRCODE_DATATYPE_MISMATCH,
            format!(
                "function {} must accept VARIADIC ANY to be used in this aggregate",
                func_signature_string(mcx, fnName, input_types)?
            ),
        ));
    }

    let mut true_oid_array = [InvalidOid; FUNC_MAX_ARGS];
    true_oid_array[..nargs].copy_from_slice(best.args.as_slice());
    let rettype = coerce::enforce_generic_type_consistency(
        input_types,
        &mut true_oid_array[..nargs],
        shape.prorettype,
        true,
    )?;

    // nodeAgg.c can't handle run-time argument coercion.
    for i in 0..nargs {
        if !coerce::IsBinaryCoercible(input_types[i], true_oid_array[i])? {
            return Err(err(
                ERRCODE_DATATYPE_MISMATCH,
                format!(
                    "function {} requires run-time type coercion",
                    func_signature_string(mcx, fnName, &true_oid_array[..nargs])?
                ),
            ));
        }
    }

    let aclresult = aclchk::object_aclcheck(
        PROCEDURE_RELATION_ID,
        fnOid,
        miscinit::GetUserId(),
        adt_acl::ACL_EXECUTE,
    )?;
    if aclresult != aclchk::ACLCHECK_OK {
        let name = lsyscache::get_func_name(mcx, fnOid)?;
        aclchk::aclcheck_error(
            aclresult,
            ObjectType::OBJECT_FUNCTION,
            name.as_ref().map(|s| s.as_str()).unwrap_or(""),
        )?;
    }

    Ok((fnOid, rettype))
}

fn name_parts<'a, 'mcx>(names: &NodeList<'mcx>, buf: &'a mut [&'mcx str; 4]) -> &'a [&'mcx str] {
    let n = names.len().min(buf.len());
    for (i, slot) in buf.iter_mut().enumerate().take(n) {
        *slot = names.nth(i).as_string().expect("name list holds String nodes").sval;
    }
    &buf[..n]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constants_match_pg_headers() {
        assert_eq!(AGGKIND_NORMAL, b'n' as i8);
        assert_eq!(AGGKIND_ORDERED_SET, b'o' as i8);
        assert_eq!(AGGKIND_HYPOTHETICAL, b'h' as i8);
        assert!(!AGGKIND_IS_ORDERED_SET(AGGKIND_NORMAL));
        assert!(AGGKIND_IS_ORDERED_SET(AGGKIND_ORDERED_SET));
        assert!(AGGKIND_IS_ORDERED_SET(AGGKIND_HYPOTHETICAL));
        assert_eq!(AGGMODIFY_READ_ONLY, b'r' as i8);
        assert_eq!(AGGMODIFY_SHAREABLE, b's' as i8);
        assert_eq!(AGGMODIFY_READ_WRITE, b'w' as i8);
        assert_eq!(AGGREGATE_RELATION_ID, 2600);
        assert_eq!(Natts_pg_aggregate, 22);
        assert_eq!(Anum_pg_aggregate_agginitval, 21);
        assert_eq!(Anum_pg_aggregate_aggminitval, 22);
    }
}
