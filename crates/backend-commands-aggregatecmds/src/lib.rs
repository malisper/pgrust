#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

//! `backend/commands/aggregatecmds.c` — CREATE AGGREGATE support
//! (PostgreSQL 18.3).
//!
//! Both C functions — the public driver [`DefineAggregate`] and the static
//! helper `extractModify` — are implemented in-crate against the owned node
//! tree, with identical branch order, permission checks, error
//! codes/messages/SQLSTATEs, and the same argument bundle handed to
//! `AggregateCreate`. The catalog-munging (`AggregateCreate`, which inserts the
//! `pg_proc`/`pg_aggregate` rows and records dependencies) crosses to
//! pg_aggregate.c through `backend-catalog-pg-aggregate-seams`, which panics
//! until its owner lands. The permission machinery, the `defGet*` accessors,
//! the new-style argument processor `interpret_function_parameter_list`, the
//! type/namespace lookups, and the initval input-function validation cross to
//! their respective owners.

use mcx::Mcx;

use backend_utils_error::ereport;
use types_error::pg_error::ErrorLocation;
use types_error::{
    PgResult, ERRCODE_INVALID_FUNCTION_DEFINITION, ERRCODE_SYNTAX_ERROR, ERROR, WARNING,
};

use backend_catalog_namespace::QualifiedNameGetCreationNamespace;
use backend_commands_define::{
    defGetBoolean, defGetInt32, defGetQualifiedName, defGetString, defGetTypeName,
};
use backend_commands_functioncmds::interpret_function_parameter_list;

use backend_catalog_aclchk_seams::{aclcheck_error, object_aclcheck};
use backend_catalog_pg_aggregate_seams::{aggregate_create, AggregateCreateArgs};
use backend_parser_parse_type_seams::{typename_to_string, typename_type_id};
use backend_utils_adt_format_type_seams::format_type_be;
use backend_utils_cache_lsyscache_seams::{get_namespace_name, get_type_input_info, get_typtype};
use backend_utils_fmgr_fmgr_seams::oid_input_function_call;
use backend_utils_init_miscinit_seams::{get_user_id, superuser};

use types_acl::{AclMode, ACLCHECK_OK, ACL_CREATE};
use types_catalog::catalog::NAMESPACE_RELATION_ID;
use types_catalog::catalog_dependency::ObjectAddress;
use types_core::primitive::{InvalidOid, Oid};
use types_core::catalog::INTERNALOID;
use types_nodes::parsenodes::OBJECT_SCHEMA;
use types_parsenodes::{
    DefElem, Node, ParseState, TypeName, AGGKIND_HYPOTHETICAL, AGGKIND_NORMAL,
    AGGKIND_ORDERED_SET, AGGMODIFY_READ_ONLY, AGGMODIFY_READ_WRITE, AGGMODIFY_SHAREABLE,
    PROPARALLEL_RESTRICTED, PROPARALLEL_SAFE, PROPARALLEL_UNSAFE, TYPTYPE_PSEUDO,
};
use types_tuple::heaptuple::{
    ANYARRAYOID, ANYCOMPATIBLEARRAYOID, ANYCOMPATIBLEMULTIRANGEOID, ANYCOMPATIBLENONARRAYOID,
    ANYCOMPATIBLEOID, ANYCOMPATIBLERANGEOID, ANYELEMENTOID, ANYENUMOID, ANYMULTIRANGEOID,
    ANYNONARRAYOID, ANYRANGEOID,
};

const OBJECT_AGGREGATE: i32 = types_nodes::parsenodes::ObjectType::Aggregate as i32;

/// Convert the raw-parser `TypeName` (`types_parsenodes`, carried in a
/// `DefElem` / returned by `defGetTypeName`) into the trimmed resolver-facing
/// `TypeName` (`types_opclass`) that the `parse_type.c` seams
/// (`typenameTypeId`/`TypeNameToString`) consume. `names` is the qualified
/// name list of `String` value nodes, flattened to the bare strings.
fn to_resolver_typename(tn: &TypeName) -> types_opclass::TypeName {
    types_opclass::TypeName {
        names: tn
            .names
            .iter()
            .map(|n| match n.as_string() {
                Some(s) => s.sval.clone().unwrap_or_default(),
                None => String::new(),
            })
            .collect(),
        typeOid: tn.typeOid,
        setof: tn.setof,
        pct_type: tn.pct_type,
        typemod: tn.typemod,
        location: tn.location,
    }
}

/// `ErrorLocation` anchored at aggregatecmds.c, for the WARNING emission.
fn errloc(lineno: i32, funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("../src/backend/commands/aggregatecmds.c", lineno, funcname)
}

/// `IsPolymorphicType(typid)` (catalog/pg_type.h) — is `typid` a polymorphic
/// pseudotype?
#[inline]
fn IsPolymorphicType(typid: Oid) -> bool {
    typid == ANYELEMENTOID
        || typid == ANYARRAYOID
        || typid == ANYNONARRAYOID
        || typid == ANYENUMOID
        || typid == ANYRANGEOID
        || typid == ANYMULTIRANGEOID
        || typid == ANYCOMPATIBLEOID
        || typid == ANYCOMPATIBLEARRAYOID
        || typid == ANYCOMPATIBLENONARRAYOID
        || typid == ANYCOMPATIBLERANGEOID
        || typid == ANYCOMPATIBLEMULTIRANGEOID
}

/// `DefineAggregate` (aggregatecmds.c:52).
///
/// "oldstyle" signals the old (pre-8.2) style where the aggregate input type is
/// specified by a BASETYPE element in the parameters. Otherwise, `args` is a
/// pair: `args[0]` is a `Node::List` of `FunctionParameter`s (the agg's args,
/// both direct and aggregated), and `args[1]` is a `Node::Integer` with the
/// number of direct args, or -1 if this isn't an ordered-set aggregate.
/// `parameters` is a list of `DefElem` representing the agg's definition
/// clauses.
//
// `sfunc1`/`stype1`/`initcond1` are accepted as exact obsolete spellings of
// `sfunc`/`stype`/`initcond`; the deliberately identical else-if arms preserve
// the 1:1 structure of the C `foreach`.
#[allow(clippy::if_same_then_else)]
#[allow(clippy::too_many_arguments)]
pub fn DefineAggregate(
    mcx: Mcx<'_>,
    pstate: &ParseState,
    name: &[Option<String>],
    args: &[Node],
    oldstyle: bool,
    parameters: &[Node],
    replace: bool,
) -> PgResult<ObjectAddress> {
    let _ = pstate;
    let mut aggKind: i8 = AGGKIND_NORMAL;
    let mut transfuncName: Vec<Node> = Vec::new();
    let mut finalfuncName: Vec<Node> = Vec::new();
    let mut combinefuncName: Vec<Node> = Vec::new();
    let mut serialfuncName: Vec<Node> = Vec::new();
    let mut deserialfuncName: Vec<Node> = Vec::new();
    let mut mtransfuncName: Vec<Node> = Vec::new();
    let mut minvtransfuncName: Vec<Node> = Vec::new();
    let mut mfinalfuncName: Vec<Node> = Vec::new();
    let mut finalfuncExtraArgs = false;
    let mut mfinalfuncExtraArgs = false;
    let mut finalfuncModify: i8 = 0;
    let mut mfinalfuncModify: i8 = 0;
    let mut sortoperatorName: Vec<Node> = Vec::new();
    let mut baseType: Option<TypeName> = None;
    let mut transType: Option<TypeName> = None;
    let mut mtransType: Option<TypeName> = None;
    let mut transSpace: i32 = 0;
    let mut mtransSpace: i32 = 0;
    let mut initval: Option<String> = None;
    let mut minitval: Option<String> = None;
    let mut parallel: Option<String> = None;
    let numArgs: i32;
    let mut numDirectArgs: i32 = 0;
    let mut proparallel: i8 = PROPARALLEL_UNSAFE;

    /* Convert list of names to a name and namespace */
    let (aggNamespace, aggName) = QualifiedNameGetCreationNamespace(mcx, name)?;
    let aggName = aggName.to_string();

    /* Check we have creation rights in target namespace */
    let aclresult = object_aclcheck::call(
        NAMESPACE_RELATION_ID,
        aggNamespace,
        get_user_id::call(),
        ACL_CREATE as AclMode,
    )?;
    if aclresult != ACLCHECK_OK {
        aclcheck_error::call(
            aclresult,
            OBJECT_SCHEMA,
            get_namespace_name::call(mcx, aggNamespace)?.map(|s| s.as_str().to_string()),
        )?;
    }

    /* Deconstruct the output of the aggr_args grammar production */
    let mut arg_list: &[Node] = &[];
    if !oldstyle {
        debug_assert_eq!(args.len(), 2);
        numDirectArgs = intVal(&args[1]);
        if numDirectArgs >= 0 {
            aggKind = AGGKIND_ORDERED_SET;
        } else {
            numDirectArgs = 0;
        }
        /* args = linitial_node(List, args) */
        arg_list = nodeAsList(&args[0]);
    }

    /* Examine aggregate's definition clauses */
    for pl in parameters {
        let defel = lfirstAsDefElem(pl);
        let defname = defel.defname.as_deref().unwrap_or("");

        /*
         * sfunc1, stype1, and initcond1 are accepted as obsolete spellings
         * for sfunc, stype, initcond.
         */
        if defname == "sfunc" {
            transfuncName = defGetQualifiedName(defel)?;
        } else if defname == "sfunc1" {
            transfuncName = defGetQualifiedName(defel)?;
        } else if defname == "finalfunc" {
            finalfuncName = defGetQualifiedName(defel)?;
        } else if defname == "combinefunc" {
            combinefuncName = defGetQualifiedName(defel)?;
        } else if defname == "serialfunc" {
            serialfuncName = defGetQualifiedName(defel)?;
        } else if defname == "deserialfunc" {
            deserialfuncName = defGetQualifiedName(defel)?;
        } else if defname == "msfunc" {
            mtransfuncName = defGetQualifiedName(defel)?;
        } else if defname == "minvfunc" {
            minvtransfuncName = defGetQualifiedName(defel)?;
        } else if defname == "mfinalfunc" {
            mfinalfuncName = defGetQualifiedName(defel)?;
        } else if defname == "finalfunc_extra" {
            finalfuncExtraArgs = defGetBoolean(defel)?;
        } else if defname == "mfinalfunc_extra" {
            mfinalfuncExtraArgs = defGetBoolean(defel)?;
        } else if defname == "finalfunc_modify" {
            finalfuncModify = extractModify(mcx, defel)?;
        } else if defname == "mfinalfunc_modify" {
            mfinalfuncModify = extractModify(mcx, defel)?;
        } else if defname == "sortop" {
            sortoperatorName = defGetQualifiedName(defel)?;
        } else if defname == "basetype" {
            baseType = Some(defGetTypeName(defel)?);
        } else if defname == "hypothetical" {
            if defGetBoolean(defel)? {
                if aggKind == AGGKIND_NORMAL {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
                        .errmsg("only ordered-set aggregates can be hypothetical")
                        .into_error());
                }
                aggKind = AGGKIND_HYPOTHETICAL;
            }
        } else if defname == "stype" {
            transType = Some(defGetTypeName(defel)?);
        } else if defname == "stype1" {
            transType = Some(defGetTypeName(defel)?);
        } else if defname == "sspace" {
            transSpace = defGetInt32(defel)?;
        } else if defname == "mstype" {
            mtransType = Some(defGetTypeName(defel)?);
        } else if defname == "msspace" {
            mtransSpace = defGetInt32(defel)?;
        } else if defname == "initcond" {
            initval = Some(defGetString(mcx, defel)?.as_str().to_string());
        } else if defname == "initcond1" {
            initval = Some(defGetString(mcx, defel)?.as_str().to_string());
        } else if defname == "minitcond" {
            minitval = Some(defGetString(mcx, defel)?.as_str().to_string());
        } else if defname == "parallel" {
            parallel = Some(defGetString(mcx, defel)?.as_str().to_string());
        } else {
            ereport(WARNING)
                .errcode(ERRCODE_SYNTAX_ERROR)
                .errmsg(format!("aggregate attribute \"{defname}\" not recognized"))
                .finish(errloc(190, "DefineAggregate"))?;
        }
    }

    /*
     * make sure we have our required definitions
     */
    if transType.is_none() {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
            .errmsg("aggregate stype must be specified")
            .into_error());
    }
    if transfuncName.is_empty() {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
            .errmsg("aggregate sfunc must be specified")
            .into_error());
    }

    /*
     * if mtransType is given, mtransfuncName and minvtransfuncName must be as
     * well; if not, then none of the moving-aggregate options should have
     * been given.
     */
    if mtransType.is_some() {
        if mtransfuncName.is_empty() {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
                .errmsg("aggregate msfunc must be specified when mstype is specified")
                .into_error());
        }
        if minvtransfuncName.is_empty() {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
                .errmsg("aggregate minvfunc must be specified when mstype is specified")
                .into_error());
        }
    } else {
        if !mtransfuncName.is_empty() {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
                .errmsg("aggregate msfunc must not be specified without mstype")
                .into_error());
        }
        if !minvtransfuncName.is_empty() {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
                .errmsg("aggregate minvfunc must not be specified without mstype")
                .into_error());
        }
        if !mfinalfuncName.is_empty() {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
                .errmsg("aggregate mfinalfunc must not be specified without mstype")
                .into_error());
        }
        if mtransSpace != 0 {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
                .errmsg("aggregate msspace must not be specified without mstype")
                .into_error());
        }
        if minitval.is_some() {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
                .errmsg("aggregate minitcond must not be specified without mstype")
                .into_error());
        }
    }

    /*
     * Default values for modify flags can only be determined once we know the
     * aggKind.
     */
    if finalfuncModify == 0 {
        finalfuncModify = if aggKind == AGGKIND_NORMAL {
            AGGMODIFY_READ_ONLY
        } else {
            AGGMODIFY_READ_WRITE
        };
    }
    if mfinalfuncModify == 0 {
        mfinalfuncModify = if aggKind == AGGKIND_NORMAL {
            AGGMODIFY_READ_ONLY
        } else {
            AGGMODIFY_READ_WRITE
        };
    }

    /*
     * look up the aggregate's input datatype(s).
     */
    let parameterTypes: Vec<Oid>;
    let allParameterTypes: Option<Vec<Oid>>;
    let parameterModes: Option<Vec<i8>>;
    let parameterNames: Option<Vec<Option<String>>>;
    let parameterDefaults: Vec<Node>;
    let variadicArgType: Oid;
    if oldstyle {
        /*
         * Old style: use basetype parameter. This supports aggregates of zero
         * or one input, with input type ANY meaning zero inputs.
         *
         * Historically we allowed the command to look like basetype = 'ANY'
         * so we must do a case-insensitive comparison for the name ANY. Ugh.
         */
        let aggArgType: Oid;

        let baseType = match baseType.as_ref() {
            Some(bt) => bt,
            None => {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
                    .errmsg("aggregate input type must be specified")
                    .into_error());
            }
        };

        let baseTypeR = to_resolver_typename(baseType);
        if pg_strcasecmp(typename_to_string::call(mcx, &baseTypeR)?.as_str(), "ANY") == 0 {
            numArgs = 0;
            aggArgType = InvalidOid;
        } else {
            numArgs = 1;
            aggArgType = typename_type_id::call(&baseTypeR)?;
        }
        parameterTypes = if numArgs == 0 {
            Vec::new()
        } else {
            vec![aggArgType]
        };
        allParameterTypes = None;
        parameterModes = None;
        parameterNames = None;
        parameterDefaults = Vec::new();
        variadicArgType = InvalidOid;
    } else {
        /*
         * New style: args is a list of FunctionParameters (possibly zero of
         * 'em). We share functioncmds.c's code for processing them.
         */
        if baseType.is_some() {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
                .errmsg("basetype is redundant with aggregate input type specification")
                .into_error());
        }

        numArgs = arg_list.len() as i32;
        let interpreted = interpret_function_parameter_list(
            arg_list,
            InvalidOid,
            OBJECT_AGGREGATE,
            false,
            false,
        )?;
        parameterTypes = interpreted.parameter_types;
        allParameterTypes = interpreted.all_parameter_types;
        parameterModes = interpreted.parameter_modes;
        parameterNames = interpreted.parameter_names;
        parameterDefaults = interpreted.parameter_defaults;
        variadicArgType = interpreted.variadic_arg_type;
        /* Parameter defaults are not currently allowed by the grammar */
        debug_assert!(parameterDefaults.is_empty());
        /* There shouldn't have been any OUT parameters, either */
        debug_assert_eq!(interpreted.required_result_type, InvalidOid);
    }

    /*
     * look up the aggregate's transtype.
     *
     * transtype can't be a pseudo-type, since we need to be able to store
     * values of the transtype. However, we can allow polymorphic transtype in
     * some cases (AggregateCreate will check). Also, we allow "internal" for
     * functions that want to pass pointers to private data structures; but
     * allow that only to superusers, since you could crash the system (or
     * worse) by connecting up incompatible internal-using functions in an
     * aggregate.
     */
    let transType = transType.as_ref().unwrap();
    let transTypeId: Oid = typename_type_id::call(&to_resolver_typename(transType))?;
    let transTypeType: i8 = get_typtype::call(transTypeId)? as i8;
    if transTypeType == TYPTYPE_PSEUDO && !IsPolymorphicType(transTypeId) {
        if transTypeId == INTERNALOID && superuser::call(mcx)? {
            /* okay */
        } else {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
                .errmsg(format!(
                    "aggregate transition data type cannot be {}",
                    format_type_be::call(mcx, transTypeId)?.as_str()
                ))
                .into_error());
        }
    }

    if !serialfuncName.is_empty() && !deserialfuncName.is_empty() {
        /*
         * Serialization is only needed/allowed for transtype INTERNAL.
         */
        if transTypeId != INTERNALOID {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
                .errmsg(format!(
                    "serialization functions may be specified only when the aggregate transition data type is {}",
                    format_type_be::call(mcx, INTERNALOID)?.as_str()
                ))
                .into_error());
        }
    } else if !serialfuncName.is_empty() || !deserialfuncName.is_empty() {
        /*
         * Cannot specify one function without the other.
         */
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
            .errmsg("must specify both or neither of serialization and deserialization functions")
            .into_error());
    }

    /*
     * If a moving-aggregate transtype is specified, look that up. Same
     * restrictions as for transtype.
     */
    let mut mtransTypeId: Oid = InvalidOid;
    let mut mtransTypeType: i8 = 0;
    if let Some(mtransType) = mtransType.as_ref() {
        mtransTypeId = typename_type_id::call(&to_resolver_typename(mtransType))?;
        mtransTypeType = get_typtype::call(mtransTypeId)? as i8;
        if mtransTypeType == TYPTYPE_PSEUDO && !IsPolymorphicType(mtransTypeId) {
            if mtransTypeId == INTERNALOID && superuser::call(mcx)? {
                /* okay */
            } else {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
                    .errmsg(format!(
                        "aggregate transition data type cannot be {}",
                        format_type_be::call(mcx, mtransTypeId)?.as_str()
                    ))
                    .into_error());
            }
        }
    }

    /*
     * If we have an initval, and it's not for a pseudotype (particularly a
     * polymorphic type), make sure it's acceptable to the type's input
     * function. We will store the initval as text, because the input function
     * isn't necessarily immutable (consider "now" for timestamp), and we want
     * to use the runtime not creation-time interpretation of the value.
     * However, if it's an incorrect value it seems much more user-friendly to
     * complain at CREATE AGGREGATE time.
     */
    if let Some(initval) = initval.as_deref() {
        if transTypeType != TYPTYPE_PSEUDO {
            let (typinput, typioparam) = get_type_input_info::call(transTypeId)?;
            let _ = oid_input_function_call::call(typinput, initval, typioparam, -1)?;
        }
    }

    /*
     * Likewise for moving-aggregate initval.
     */
    if let Some(minitval) = minitval.as_deref() {
        if mtransTypeType != TYPTYPE_PSEUDO {
            let (typinput, typioparam) = get_type_input_info::call(mtransTypeId)?;
            let _ = oid_input_function_call::call(typinput, minitval, typioparam, -1)?;
        }
    }

    if let Some(parallel) = parallel.as_deref() {
        if parallel == "safe" {
            proparallel = PROPARALLEL_SAFE;
        } else if parallel == "restricted" {
            proparallel = PROPARALLEL_RESTRICTED;
        } else if parallel == "unsafe" {
            proparallel = PROPARALLEL_UNSAFE;
        } else {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_SYNTAX_ERROR)
                .errmsg("parameter \"parallel\" must be SAFE, RESTRICTED, or UNSAFE")
                .into_error());
        }
    }

    /*
     * Most of the argument-checking is done inside of AggregateCreate
     */
    aggregate_create::call(AggregateCreateArgs {
        agg_name: aggName,           /* aggregate name */
        agg_namespace: aggNamespace, /* namespace */
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
        transfunc_name: transfuncName,         /* step function name */
        finalfunc_name: finalfuncName,         /* final function name */
        combinefunc_name: combinefuncName,     /* combine function name */
        serialfunc_name: serialfuncName,       /* serial function name */
        deserialfunc_name: deserialfuncName,   /* deserial function name */
        mtransfunc_name: mtransfuncName,       /* fwd trans function name */
        minvtransfunc_name: minvtransfuncName, /* inv trans function name */
        mfinalfunc_name: mfinalfuncName,       /* final function name */
        finalfunc_extra_args: finalfuncExtraArgs,
        mfinalfunc_extra_args: mfinalfuncExtraArgs,
        finalfunc_modify: finalfuncModify,
        mfinalfunc_modify: mfinalfuncModify,
        sortoperator_name: sortoperatorName, /* sort operator name */
        trans_type_id: transTypeId,          /* transition data type */
        trans_space: transSpace,             /* transition space */
        mtrans_type_id: mtransTypeId,        /* transition data type */
        mtrans_space: mtransSpace,           /* transition space */
        initval,                             /* initial condition */
        minitval,                            /* initial condition */
        proparallel,                         /* parallel safe? */
    })
}

/// `extractModify` (aggregatecmds.c:477) — convert the string form of
/// `[m]finalfunc_modify` to the catalog representation.
fn extractModify(mcx: Mcx<'_>, defel: &DefElem) -> PgResult<i8> {
    let val = defGetString(mcx, defel)?;
    let val = val.as_str();

    if val == "read_only" {
        return Ok(AGGMODIFY_READ_ONLY);
    }
    if val == "shareable" {
        return Ok(AGGMODIFY_SHAREABLE);
    }
    if val == "read_write" {
        return Ok(AGGMODIFY_READ_WRITE);
    }
    let defname = defel.defname.as_deref().unwrap_or("");
    Err(ereport(ERROR)
        .errcode(ERRCODE_SYNTAX_ERROR)
        .errmsg(format!(
            "parameter \"{defname}\" must be READ_ONLY, SHAREABLE, or READ_WRITE"
        ))
        .into_error())
    /* the C `return 0;` is unreachable after ereport(ERROR) */
}

/// `intVal(node)` — read the `ival` of an `Integer` value node (the second
/// element of the new-style `aggr_args` pair).
fn intVal(node: &Node) -> i32 {
    match node.as_integer() {
        Some(i) => i.ival,
        None => panic!("aggregatecmds: aggr_args pair second element is not an Integer node"),
    }
}

/// `linitial_node(List, args)` — the inner `FunctionParameter` list carried by
/// the first cell of the new-style `aggr_args` pair.
fn nodeAsList(node: &Node) -> &[Node] {
    match node {
        Node::List(cells) => cells.as_slice(),
        _ => panic!("aggregatecmds: aggr_args pair first element is not a List node"),
    }
}

/// `lfirst_node(DefElem, pl)` — borrow a definition-clause cell's `DefElem`.
fn lfirstAsDefElem(node: &Node) -> &DefElem {
    match node.as_defelem() {
        Some(d) => d,
        None => panic!("aggregatecmds: aggregate definition clause is not a DefElem node"),
    }
}

/// `pg_strcasecmp(s1, s2)` — ASCII case-insensitive comparison returning the
/// sign of the difference (0 when equal).
fn pg_strcasecmp(s1: &str, s2: &str) -> i32 {
    let mut a = s1.bytes();
    let mut b = s2.bytes();
    loop {
        let ca = a.next();
        let cb = b.next();
        match (ca, cb) {
            (None, None) => return 0,
            (Some(x), Some(y)) => {
                let lx = x.to_ascii_lowercase();
                let ly = y.to_ascii_lowercase();
                if lx != ly {
                    return lx as i32 - ly as i32;
                }
            }
            (None, Some(y)) => return -(y.to_ascii_lowercase() as i32),
            (Some(x), None) => return x.to_ascii_lowercase() as i32,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn agg_constants_match_pg_headers() {
        assert_eq!(AGGKIND_NORMAL, b'n' as i8);
        assert_eq!(AGGKIND_ORDERED_SET, b'o' as i8);
        assert_eq!(AGGKIND_HYPOTHETICAL, b'h' as i8);
        assert_eq!(AGGMODIFY_READ_ONLY, b'r' as i8);
        assert_eq!(AGGMODIFY_SHAREABLE, b's' as i8);
        assert_eq!(AGGMODIFY_READ_WRITE, b'w' as i8);
        assert_eq!(PROPARALLEL_SAFE, b's' as i8);
        assert_eq!(PROPARALLEL_RESTRICTED, b'r' as i8);
        assert_eq!(PROPARALLEL_UNSAFE, b'u' as i8);
        assert_eq!(TYPTYPE_PSEUDO, b'p' as i8);
        assert_eq!(INTERNALOID, 2281);
        assert_eq!(OBJECT_AGGREGATE, 1);
    }

    #[test]
    fn polymorphic_type_oids_match_pg_type_dat() {
        for oid in [
            2283, 2277, 2776, 3500, 3831, 4537, 5077, 5078, 5079, 5080, 4538,
        ] {
            assert!(IsPolymorphicType(oid), "oid {oid} should be polymorphic");
        }
        assert!(!IsPolymorphicType(INTERNALOID));
        assert!(!IsPolymorphicType(23 /* int4 */));
    }

    #[test]
    fn pg_strcasecmp_matches_c() {
        assert_eq!(pg_strcasecmp("ANY", "any"), 0);
        assert_eq!(pg_strcasecmp("ANY", "ANY"), 0);
        assert!(pg_strcasecmp("int4", "ANY") != 0);
        assert!(pg_strcasecmp("a", "b") < 0);
        assert!(pg_strcasecmp("b", "a") > 0);
        assert!(pg_strcasecmp("an", "any") < 0);
    }
}
